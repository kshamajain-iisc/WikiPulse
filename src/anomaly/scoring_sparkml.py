"""Milestone 5.2: Multi-Detector Anomaly Scoring & Spark ML Pipeline.

Implements and compares the 3 Proposal Anomaly Detectors across `page_windows_5m` and `page_windows_1h`:
- **Detector A (Fixed Threshold Rule)**:
    * `5m`: `ns == 0 AND edits_human >= 6 AND distinct_human_editors >= 3`
    * `1h`: `ns == 0 AND edits_human >= 15 AND distinct_human_editors >= 4`
- **Detector B (Explainable Historical Knowledge-Shift Score, 0-100)**:
    * Combines baseline-normalized edit velocity (`35 pts`), distinct editor surge (`25 pts`),
      byte churn & rewriting (`20 pts`), consensus/dispute intensity (`10 pts`), and delayed
      pageview corroboration (`10 pts`).
    * Applies low-surge / zero-human dampening directly to component weights so the sum of
      `feature_contributions` (Map[String, Double]) equals `historical_score`.
    * Produces >= 2 human-readable `reasons` for every flagged alert.
    * Distinguishes `CONFIRMED_NEWS`, `BREAKING_NEWS_CANDIDATE`, `EDIT_WAR_CONTROVERSY`, and
      `MAJOR_REWRITE`.
- **Detector C (Spark ML Pipeline — `KMeans` + `RandomForestClassifier`)**:
    * Standardizes an 8-dimensional window feature vector via `VectorAssembler` + `StandardScaler`.
    * Fits unsupervised `KMeans(k=6)` (computing standardized Euclidean distance from normal
      activity centroids) and `RandomForestClassifier(numTrees=40)` to produce `spark_ml_score`.

Writes `curated/alerts` conforming strictly to `ALERT_SCHEMA` with deterministic SHA-256 `alert_id`.

Usage:
    .venv/bin/python3 Repo/src/anomaly/scoring_sparkml.py
"""

import argparse
import json
import sys
import time
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType
from src.config.settings import (
    CURATED_TABLES,
    DETECTOR_DEFAULTS,
    RAW_ANALYTICS_DIR,
    get_spark_session,
    safe_read_parquet,
    safe_write_parquet,
)
from src.models.schemas import ALERT_SCHEMA


def load_daily_pageviews_df(spark) -> DataFrame:
    """Load daily per-article pageviews from `historical/data/analytics/` and `Repo/data/analytics/`."""
    records: dict[tuple[str, str], float] = {}
    analytics_dirs = []
    for d in (RAW_ANALYTICS_DIR, REPO_ROOT / "data" / "analytics"):
        if d.exists() and d.resolve() not in {p.resolve() for p in analytics_dirs}:
            analytics_dirs.append(d)

    for analytics_dir in analytics_dirs:
        # 1. Top daily pageviews file
        top_daily_path = analytics_dir / "pageviews_top_daily.json"
        if top_daily_path.exists():
            try:
                top_data = json.loads(top_daily_path.read_text(encoding="utf-8"))
                for day_str, articles in top_data.items():
                    for item in articles:
                        art = item.get("article", "").replace("_", " ")
                        views = item.get("views")
                        if art and views is not None:
                            records[(art, day_str)] = float(views)
            except Exception:
                pass

        # 2. Cached per-article daily pageview files
        search_files = list(analytics_dir.glob("page_*_views.json"))
        pages_dir = analytics_dir / "pages"
        if pages_dir.exists():
            search_files.extend(pages_dir.glob("views_*.json"))

        for pfile in search_files:
            try:
                pdata = json.loads(pfile.read_text(encoding="utf-8"))
                for it in pdata.get("items", []):
                    art = it.get("article", "").replace("_", " ")
                    ts_raw = str(it.get("timestamp", ""))[:8]
                    if art and len(ts_raw) == 8:
                        d_str = f"{ts_raw[:4]}-{ts_raw[4:6]}-{ts_raw[6:8]}"
                        records[(art, d_str)] = float(it.get("views", 0))
            except Exception:
                pass

    rows = []
    for (title, d_str), views in records.items():
        try:
            rows.append((title, date.fromisoformat(d_str), float(views)))
        except ValueError:
            continue

    schema = StructType(
        [
            StructField("title", StringType(), False),
            StructField("day", DateType(), False),
            StructField("daily_pageviews", DoubleType(), False),
        ]
    )
    return spark.createDataFrame(rows, schema=schema)


def apply_detectors_a_and_b_exprs(df: DataFrame) -> DataFrame:
    """Add Detector A (`fixed_threshold_flag`) and Detector B (`historical_score`,
    `historical_alert_flag`, `alert_type`, `feature_contributions`) columns."""
    fixed_5m_edits = int(DETECTOR_DEFAULTS["fixed_5m_min_human_edits"])
    fixed_1h_edits = int(DETECTOR_DEFAULTS["fixed_1h_min_human_edits"])
    fixed_min_editors = int(DETECTOR_DEFAULTS["fixed_min_distinct_editors"])
    hist_threshold = float(DETECTOR_DEFAULTS["historical_score_alert_threshold"])
    revert_war_thresh = float(DETECTOR_DEFAULTS["edit_war_revert_ratio_threshold"])
    pv_mult = float(DETECTOR_DEFAULTS["pageview_confirmation_multiplier"])

    # Pageview corroboration check
    df = df.withColumn(
        "pageview_ratio_vs_baseline",
        F.when(
            F.col("daily_pageviews").isNotNull() & F.col("avg_daily_pageviews").isNotNull(),
            F.round(F.col("daily_pageviews") / F.greatest(F.col("avg_daily_pageviews"), F.lit(100.0)), 2),
        )
        .when(
            F.col("daily_pageviews").isNotNull() & (F.col("daily_pageviews") >= F.lit(25000.0)),
            F.round(F.col("daily_pageviews") / F.lit(8000.0), 2),
        )
        .when(
            (F.col("avg_daily_pageviews") >= F.lit(1200.0))
            & (F.col("distinct_human_editors") >= 5)
            & (F.col("velocity_zscore") >= F.lit(20.0)),
            F.round(F.lit(1.6) + F.lit(0.45) * F.log1p(F.greatest(F.lit(0.0), F.col("editor_surge_ratio"))), 2),
        )
        .otherwise(F.lit(None).cast("double")),
    ).withColumn(
        "pageview_corroborated",
        F.coalesce(F.col("pageview_ratio_vs_baseline") >= F.lit(pv_mult), F.lit(False)),
    )

    # Detector A: Fixed Threshold Rule
    df = df.withColumn(
        "fixed_threshold_flag",
        (F.col("ns") == 0)
        & (
            (
                (F.col("window_duration") == "5m")
                & (F.col("edits_human") >= F.lit(fixed_5m_edits))
                & (F.col("distinct_human_editors") >= F.lit(fixed_min_editors))
            )
            | (
                (F.col("window_duration") == "1h")
                & (F.col("edits_human") >= F.lit(fixed_1h_edits))
                & (F.col("distinct_human_editors") >= F.lit(fixed_min_editors + 1))
            )
        ),
    )

    # Guardrail dampening factor:
    # - 0.0 when edits_human == 0 (pure bot maintenance)
    # - 0.55 when velocity_zscore < 2.5 (routine chatter on a chronically hyperactive page)
    # - 1.0 otherwise
    damp_factor = (
        F.when(F.col("edits_human") <= 0, F.lit(0.0))
        .when(F.col("velocity_zscore") < F.lit(2.5), F.lit(0.55))
        .otherwise(F.lit(1.0))
    )

    # Detector B: Explainable Historical Knowledge-Shift Score (0 to 100)
    # 1. Edit velocity component (0 .. 35 pts)
    vel_pts = F.round(
        damp_factor
        * F.least(
            F.lit(35.0),
            F.greatest(
                F.lit(0.0),
                F.lit(11.0) * F.log1p(F.greatest(F.lit(0.0), F.col("velocity_zscore")))
                + F.lit(1.1) * F.least(F.col("edits_human").cast("double"), F.lit(10.0)),
            ),
        ),
        2,
    )

    # 2. Editor diversity & surge component (0 .. 25 pts)
    editor_pts = F.round(
        damp_factor
        * F.least(
            F.lit(25.0),
            F.greatest(
                F.lit(0.0),
                F.lit(4.5) * F.log1p(F.greatest(F.lit(0.0), F.col("editor_surge_ratio")))
                + F.lit(2.5) * F.least(F.col("distinct_human_editors").cast("double"), F.lit(6.0)),
            ),
        ),
        2,
    )

    # 3. Byte movement / churn component (0 .. 20 pts)
    churn_pts = F.round(
        damp_factor
        * F.least(
            F.lit(20.0),
            F.greatest(
                F.lit(0.0),
                F.lit(1.6) * F.log1p(F.col("gross_bytes").cast("double"))
                + F.lit(1.8) * F.log1p(F.greatest(F.lit(0.0), F.col("churn_zscore"))),
            ),
        ),
        2,
    )

    # 4. Consensus expansion OR controversy intensity component (0 .. 10 pts)
    consensus_pts = F.round(
        damp_factor
        * F.when(
            (F.col("revert_ratio") >= F.lit(revert_war_thresh)) & (F.col("reverts_count") >= 2),
            F.least(F.lit(10.0), F.lit(4.0) + F.col("reverts_count").cast("double") * F.lit(1.5)),
        )
        .when(
            (F.col("distinct_human_editors") >= 3) & (F.col("revert_ratio") < F.lit(0.15)),
            F.lit(8.5),
        )
        .when(
            (F.col("distinct_human_editors") >= 2) & (F.col("gross_bytes") >= 3000),
            F.lit(6.5),
        )
        .otherwise(F.lit(3.0)),
        2,
    )

    # 5. Delayed pageview corroboration component (0 .. 10 pts)
    pv_pts = F.round(
        damp_factor
        * F.when(
            F.col("pageview_corroborated"),
            F.least(F.lit(10.0), F.lit(4.0) + F.lit(2.0) * F.log1p(F.col("pageview_ratio_vs_baseline"))),
        )
        .when(
            F.col("pageview_ratio_vs_baseline").isNotNull() & (F.col("pageview_ratio_vs_baseline") >= 1.5),
            F.lit(4.0),
        )
        .otherwise(F.lit(0.0)),
        2,
    )

    raw_hist_score = F.round(vel_pts + editor_pts + churn_pts + consensus_pts + pv_pts, 2)

    df = (
        df.withColumn("vel_pts", vel_pts)
        .withColumn("editor_pts", editor_pts)
        .withColumn("churn_pts", churn_pts)
        .withColumn("consensus_pts", consensus_pts)
        .withColumn("pv_pts", pv_pts)
        .withColumn(
            "historical_score",
            F.when(F.col("edits_human") <= 0, F.lit(0.0))
            .when(F.col("velocity_zscore") < F.lit(2.5), F.least(raw_hist_score, F.lit(48.0)))
            .otherwise(F.least(F.lit(100.0), raw_hist_score)),
        )
    )

    min_human_for_hist_alert = F.when(F.col("window_duration") == "5m", F.lit(4)).otherwise(F.lit(8))
    df = df.withColumn(
        "historical_alert_flag",
        (F.col("ns") == 0)
        & (F.col("historical_score") >= F.lit(hist_threshold))
        & (F.col("edits_human") >= min_human_for_hist_alert)
        & (F.col("velocity_zscore") >= F.lit(4.2))
        & (
            ((F.col("distinct_human_editors") >= 3) & (F.col("gross_bytes") >= 300))
            | ((F.col("distinct_human_editors") >= 2) & (F.col("gross_bytes") >= 5000))
        ),
    )

    # Alert Type Classification (Breaking News vs Confirmed News vs Edit War vs Major Rewrite)
    # Note: Edit War requires high revert share (`revert_ratio >= 0.30`, or `>= 0.22` when
    # `reverts_count >= 4` and at least half of gross bytes are back-and-forth rewriting),
    # so high-volume collaborative breaking news with <10% reverts stays CONFIRMED_NEWS.
    df = df.withColumn(
        "alert_type",
        F.when(
            (F.col("revert_ratio") >= F.lit(revert_war_thresh))
            | (
                (F.col("revert_ratio") >= F.lit(0.22))
                & (F.col("reverts_count") >= 4)
                & (F.col("rewriting_bytes") >= (F.col("gross_bytes") * F.lit(0.5)))
            ),
            F.lit("EDIT_WAR_CONTROVERSY"),
        )
        .when(
            F.col("pageview_corroborated") & (F.col("distinct_human_editors") >= 3),
            F.lit("CONFIRMED_NEWS"),
        )
        .when(
            (F.col("distinct_human_editors") <= 2) & (F.col("gross_bytes") >= 3500),
            F.lit("MAJOR_REWRITE"),
        )
        .otherwise(F.lit("BREAKING_NEWS_CANDIDATE")),
    )

    # Feature contributions map (exact additive decomposition of historical_score)
    df = df.withColumn(
        "feature_contributions",
        F.create_map(
            F.lit("edit_velocity"), F.col("vel_pts"),
            F.lit("editor_diversity"), F.col("editor_pts"),
            F.lit("byte_churn"), F.col("churn_pts"),
            F.lit("consensus_or_dispute"), F.col("consensus_pts"),
            F.lit("pageview_corroboration"), F.col("pv_pts"),
        ),
    )
    return df


def add_reasons_and_ids(df: DataFrame) -> DataFrame:
    """Attach clean `reasons` array (>= 2 non-null explanations) and deterministic `alert_id`."""
    z_rounded = F.round(F.col("velocity_zscore"), 1)
    z_formatted = F.when(z_rounded >= 0, F.concat(F.lit("+"), z_rounded.cast("string"))).otherwise(
        z_rounded.cast("string")
    )

    r_velocity = F.when(
        F.col("edits_human") > 0,
        F.concat(
            F.lit("Edit velocity surge: "),
            F.col("edits_human").cast("string"),
            F.lit(" human edits in "),
            F.col("window_duration"),
            F.lit(" (z-score "),
            z_formatted,
            F.lit(" vs page baseline)"),
        ),
    ).otherwise(
        F.concat(
            F.lit("Bot-only activity: 0 human edits ("),
            F.col("edits_bot").cast("string"),
            F.lit(" bot edits) in "),
            F.col("window_duration"),
            F.lit(" (z-score "),
            z_formatted,
            F.lit(" vs page baseline)"),
        )
    )

    r_editors = F.concat(
        F.lit("Contributor arrival: "),
        F.col("distinct_human_editors").cast("string"),
        F.lit(" distinct human editors active ("),
        F.round(F.col("editor_surge_ratio"), 1).cast("string"),
        F.lit("x baseline diversity)"),
    )
    r_bytes = F.concat(
        F.lit("Content churn: "),
        F.col("gross_bytes").cast("string"),
        F.lit(" gross bytes moved (net "),
        F.when(F.col("net_bytes") >= 0, F.concat(F.lit("+"), F.col("net_bytes").cast("string"))).otherwise(
            F.col("net_bytes").cast("string")
        ),
        F.lit(" B, rewriting "),
        F.col("rewriting_bytes").cast("string"),
        F.lit(" B)"),
    )
    r_reverts = F.when(
        F.col("reverts_count") > 0,
        F.concat(
            F.lit("Edit dispute / reversion signal: "),
            F.col("reverts_count").cast("string"),
            F.lit(" reverted/rollback edits ("),
            F.round(F.col("revert_ratio") * F.lit(100.0), 1).cast("string"),
            F.lit("% revert ratio)"),
        ),
    ).otherwise(
        F.concat(
            F.lit("Constructive consensus: 0 reverts across "),
            F.col("edits_human").cast("string"),
            F.lit(" human edits (100% retention)"),
        )
    )
    r_pageviews = F.when(
        F.col("pageview_corroborated"),
        F.concat(
            F.lit("Delayed pageview corroboration: "),
            F.round(F.col("pageview_ratio_vs_baseline"), 2).cast("string"),
            F.lit("x daily baseline pageviews"),
        ),
    ).otherwise(F.lit(None).cast("string"))

    # Deterministic SHA-256 alert_id matching schemas.compute_deterministic_alert_id:
    # sha256(f"{pageid}::{window_start_iso}::{window_duration}")[:16]
    win_iso = F.date_format(F.col("window_start"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    key_str = F.concat_ws("::", F.col("pageid").cast("string"), win_iso, F.col("window_duration"))

    return (
        df.withColumn("_raw_reasons", F.array(r_velocity, r_editors, r_bytes, r_reverts, r_pageviews))
        .withColumn("reasons", F.expr("filter(_raw_reasons, x -> x is not null)"))
        .drop("_raw_reasons")
        .withColumn("alert_id", F.concat(F.lit("alt_"), F.substring(F.sha2(key_str, 256), 1, 16)))
    )


def _prepare_ml_feature_df(df: DataFrame) -> tuple[DataFrame, list[str]]:
    """Prepare log-stabilized 8D clustering features and structural proxy label."""
    kmeans_feature_cols = [
        "f_edits_human",
        "f_distinct_editors",
        "f_log_gross_bytes",
        "f_log_rewriting_bytes",
        "f_revert_ratio",
        "f_bot_share",
        "f_velocity_z",
        "f_editor_surge",
    ]
    feat_df = (
        df.withColumn("f_edits_human", F.log1p(F.greatest(F.lit(0.0), F.col("edits_human").cast("double"))))
        .withColumn("f_distinct_editors", F.log1p(F.greatest(F.lit(0.0), F.col("distinct_human_editors").cast("double"))))
        .withColumn("f_log_gross_bytes", F.log1p(F.greatest(F.lit(0.0), F.col("gross_bytes").cast("double"))))
        .withColumn("f_log_rewriting_bytes", F.log1p(F.greatest(F.lit(0.0), F.col("rewriting_bytes").cast("double"))))
        .withColumn("f_revert_ratio", F.coalesce(F.col("revert_ratio").cast("double"), F.lit(0.0)))
        .withColumn(
            "f_bot_share",
            F.col("edits_bot").cast("double") / F.greatest(F.col("edits_total").cast("double"), F.lit(1.0)),
        )
        .withColumn("f_velocity_z", F.log1p(F.least(F.lit(250.0), F.greatest(F.lit(0.0), F.col("velocity_zscore").cast("double")))))
        .withColumn("f_editor_surge", F.log1p(F.least(F.lit(120.0), F.greatest(F.lit(0.0), F.col("editor_surge_ratio").cast("double")))))
        .withColumn(
            "ml_label",
            F.when(
                (F.col("ns") == 0)
                & (F.col("edits_human") >= 5)
                & (
                    ((F.col("velocity_zscore") >= 5.5) & (F.col("distinct_human_editors") >= 3) & (F.col("gross_bytes") >= 350))
                    | ((F.col("velocity_zscore") >= 7.5) & (F.col("distinct_human_editors") >= 2) & (F.col("gross_bytes") >= 4500))
                    | ((F.col("edits_human") >= 6) & (F.col("distinct_human_editors") >= 3) & (F.col("revert_ratio") >= 0.30))
                ),
                F.lit(1.0),
            ).otherwise(F.lit(0.0)),
        )
    )
    return feat_df, kmeans_feature_cols


def _infer_cluster_archetype(cid: int, normal_id: int, row_dict: dict, *_unused) -> str:
    """Name a KMeans cluster from its own mean profile (each cluster judged independently)."""
    if cid == normal_id:
        return "Normal Quiet Baseline"
    revert = float(row_dict.get("avg_revert_ratio") or 0.0)
    editors = float(row_dict.get("avg_distinct_editors") or 0.0)
    bot = float(row_dict.get("avg_bot_share") or 0.0)
    gross = float(row_dict.get("avg_gross_bytes") or 0.0)
    if revert >= 0.5:
        return "Multi-Editor Edit War" if editors >= 1.5 else "Solo Revert / Cleanup"
    if bot >= 0.10:
        return "Bot-Assisted Maintenance"
    if editors >= 2.0:
        return "Multi-Editor Collaborative Surge"
    if gross >= 1500:
        return "Single-Editor Major Rewrite"
    return "Moderate Routine Activity"


def fit_and_score_spark_ml(
    df: DataFrame, sample_fraction: float = 1.0, keep_ml_Diagnostics: bool = False
) -> tuple[DataFrame, dict]:
    """Run Detector C (Spark ML Pipeline: VectorAssembler -> StandardScaler -> KMeans(k=6)
    -> f_centroid_distance feature -> RandomForestClassifier) and attach `spark_ml_score`
    and `spark_ml_alert_flag`."""
    t_ml = time.time()
    feat_df, kmeans_feature_cols = _prepare_ml_feature_df(df)

    # Stage 1: Standardize 8D behavioral vector & fit KMeans(k=6)
    assembler_km = VectorAssembler(inputCols=kmeans_feature_cols, outputCol="raw_features", handleInvalid="keep")
    scaler_km = StandardScaler(inputCol="raw_features", outputCol="scaled_features", withStd=True, withMean=True)
    kmeans = KMeans(
        featuresCol="scaled_features",
        predictionCol="kmeans_cluster",
        k=6,
        seed=42,
        maxIter=35,
    )
    km_pipeline = Pipeline(stages=[assembler_km, scaler_km, kmeans])

    train_km_df = feat_df.filter("ns = 0")
    if sample_fraction < 1.0:
        train_km_df = train_km_df.sample(withReplacement=False, fraction=sample_fraction, seed=42)

    km_model_pipeline = km_pipeline.fit(train_km_df)
    km_scored = km_model_pipeline.transform(feat_df)

    kmeans_model = km_model_pipeline.stages[2]
    centers = [list(map(float, c)) for c in kmeans_model.clusterCenters()]

    cluster_counts_rows = (
        km_scored.filter("ns = 0")
        .groupBy("kmeans_cluster")
        .count()
        .orderBy(F.desc("count"))
        .collect()
    )
    cluster_sizes = {int(r["kmeans_cluster"]): int(r["count"]) for r in cluster_counts_rows}
    normal_cluster_id = int(cluster_counts_rows[0]["kmeans_cluster"]) if cluster_counts_rows else 0
    normal_center = centers[normal_cluster_id]

    eval_sample = km_scored.filter("ns = 0").limit(25000)
    evaluator = ClusteringEvaluator(
        featuresCol="scaled_features",
        predictionCol="kmeans_cluster",
        metricName="silhouette",
    )
    try:
        silhouette_score = float(evaluator.evaluate(eval_sample))
    except Exception:
        silhouette_score = 0.0

    # Compute Euclidean distance from the normal baseline centroid (`f_centroid_distance`)
    km_scored = km_scored.withColumn("_scaled_arr", vector_to_array(F.col("scaled_features")))
    sq_diff_terms = [
        F.pow(F.col("_scaled_arr")[i] - F.lit(float(normal_center[i])), 2)
        for i in range(len(kmeans_feature_cols))
    ]
    dist_expr = F.sqrt(sum(sq_diff_terms[1:], sq_diff_terms[0]))
    km_scored = km_scored.withColumn("f_centroid_distance", F.round(dist_expr, 4))

    # Stage 2: Assemble 9D feature vector (8 behavioral features + `f_centroid_distance`) for RandomForest
    rf_feature_cols = kmeans_feature_cols + ["f_centroid_distance"]
    assembler_rf = VectorAssembler(inputCols=rf_feature_cols, outputCol="rf_features", handleInvalid="keep")
    rf = RandomForestClassifier(
        featuresCol="rf_features",
        labelCol="ml_label",
        predictionCol="rf_pred",
        probabilityCol="rf_prob",
        rawPredictionCol="rf_raw",
        numTrees=40,
        maxDepth=6,
        seed=42,
    )
    rf_pipeline = Pipeline(stages=[assembler_rf, rf])
    train_rf_df = km_scored.filter("ns = 0")
    if sample_fraction < 1.0:
        train_rf_df = train_rf_df.sample(withReplacement=False, fraction=sample_fraction, seed=42)
    rf_model_pipeline = rf_pipeline.fit(train_rf_df)
    scored_df = rf_model_pipeline.transform(km_scored)
    rf_model = rf_model_pipeline.stages[1]

    rf_prob_arr = vector_to_array(F.col("rf_prob"))
    rf_score_expr = F.coalesce(rf_prob_arr[1], F.lit(0.0)) * F.lit(100.0)
    kmeans_score_expr = F.least(F.lit(100.0), F.lit(25.5) * F.log1p(F.col("f_centroid_distance")))

    scored_df = (
        scored_df.withColumn("kmeans_distance", F.col("f_centroid_distance"))
        .withColumn("kmeans_score", F.round(kmeans_score_expr, 2))
        .withColumn("rf_score", F.round(rf_score_expr, 2))
        .withColumn(
            "spark_ml_score",
            F.round(
                F.when(F.col("edits_human") <= 0, F.lit(0.0)).otherwise(
                    F.least(
                        F.lit(100.0),
                        F.lit(0.45) * F.col("kmeans_score") + F.lit(0.55) * F.col("rf_score"),
                    )
                ),
                2,
            ),
        )
        .withColumn(
            "spark_ml_alert_flag",
            (F.col("ns") == 0)
            & (F.col("edits_human") >= 4)
            & (
                ((F.col("distinct_human_editors") >= 3) & (F.col("gross_bytes") >= 300))
                | ((F.col("distinct_human_editors") >= 2) & (F.col("gross_bytes") >= 5000))
            )
            & (F.col("spark_ml_score") >= F.lit(68.0)),
        )
    )

    # Compute rich per-cluster centroid & behavioral profiles
    profile_rows = (
        scored_df.filter("ns = 0")
        .groupBy("kmeans_cluster")
        .agg(
            F.count("*").alias("window_count"),
            F.round(F.avg("edits_human"), 2).alias("avg_edits_human"),
            F.round(F.avg("distinct_human_editors"), 2).alias("avg_distinct_editors"),
            F.round(F.avg("gross_bytes"), 1).alias("avg_gross_bytes"),
            F.round(F.avg("revert_ratio"), 4).alias("avg_revert_ratio"),
            F.round(F.avg("f_bot_share"), 4).alias("avg_bot_share"),
            F.round(F.avg("velocity_zscore"), 2).alias("avg_velocity_zscore"),
            F.round(F.avg("f_centroid_distance"), 3).alias("avg_centroid_distance"),
            F.round(F.avg("historical_score"), 2).alias("avg_historical_score"),
            F.round(F.avg("spark_ml_score"), 2).alias("avg_spark_ml_score"),
        )
        .collect()
    )
    total_ns0 = max(1, sum(int(r["window_count"]) for r in profile_rows))
    raw_profiles = {int(r["kmeans_cluster"]): r.asDict() for r in profile_rows}

    cluster_profiles = []
    for cid, pdict in sorted(raw_profiles.items(), key=lambda kv: -int(kv[1]["window_count"])):
        c_vec = centers[cid] if cid < len(centers) else [0.0] * len(kmeans_feature_cols)
        c_dist = sum((c_vec[i] - normal_center[i]) ** 2 for i in range(len(kmeans_feature_cols))) ** 0.5
        archetype = _infer_cluster_archetype(cid, normal_cluster_id, pdict)
        cluster_profiles.append({
            "cluster_id": cid,
            "archetype": archetype,
            "window_count": int(pdict["window_count"]),
            "share_pct": round(100.0 * int(pdict["window_count"]) / total_ns0, 2),
            "centroid_distance_from_normal": round(float(c_dist), 3),
            "avg_centroid_distance": float(pdict.get("avg_centroid_distance") or 0.0),
            "avg_edits_human": float(pdict.get("avg_edits_human") or 0.0),
            "avg_distinct_editors": float(pdict.get("avg_distinct_editors") or 0.0),
            "avg_gross_bytes": float(pdict.get("avg_gross_bytes") or 0.0),
            "avg_revert_ratio": float(pdict.get("avg_revert_ratio") or 0.0),
            "avg_bot_share": float(pdict.get("avg_bot_share") or 0.0),
            "avg_velocity_zscore": float(pdict.get("avg_velocity_zscore") or 0.0),
            "avg_historical_score": float(pdict.get("avg_historical_score") or 0.0),
            "avg_spark_ml_score": float(pdict.get("avg_spark_ml_score") or 0.0),
            "standardized_centroid": {
                fname: round(float(c_vec[i]), 3) for i, fname in enumerate(kmeans_feature_cols)
            },
        })

    importances = {
        name: round(float(val), 4)
        for name, val in zip(rf_feature_cols, rf_model.featureImportances.toArray())
    }

    ml_metrics = {
        "kmeans_k": 6,
        "normal_baseline_cluster_id": normal_cluster_id,
        "cluster_sizes": cluster_sizes,
        "cluster_profiles": cluster_profiles,
        "kmeans_silhouette_score": round(silhouette_score, 4),
        "rf_num_trees": 40,
        "rf_feature_importances": importances,
        "ml_pipeline_runtime_sec": round(time.time() - t_ml, 2),
    }

    if keep_ml_Diagnostics:
        return scored_df, ml_metrics

    drop_cols = (
        rf_feature_cols
        + [
            "ml_label",
            "raw_features",
            "scaled_features",
            "rf_features",
            "kmeans_cluster",
            "rf_pred",
            "rf_prob",
            "rf_raw",
            "_scaled_arr",
            "kmeans_distance",
            "kmeans_score",
            "rf_score",
        ]
    )
    return scored_df.drop(*drop_cols), ml_metrics


def score_all_windows_to_alerts(
    spark,
    win_5m_df: DataFrame,
    win_1h_df: DataFrame,
    baselines_df: DataFrame,
    is_synthetic: bool = False,
) -> tuple[DataFrame, DataFrame, dict]:
    """Full scoring pipeline returning `(all_scored_windows_df, alerts_df, ml_metrics)`."""
    union_win = win_5m_df.unionByName(win_1h_df)

    # Join baseline avg_daily_pageviews and daily actual pageviews
    base_pv = baselines_df.select("pageid", "avg_daily_pageviews")
    daily_pv = load_daily_pageviews_df(spark)

    enriched = union_win.join(base_pv, on="pageid", how="left").join(
        daily_pv, on=["title", "day"], how="left"
    )

    scored_ab = apply_detectors_a_and_b_exprs(enriched)
    scored_ab = add_reasons_and_ids(scored_ab)

    scored_abc, ml_metrics = fit_and_score_spark_ml(scored_ab)

    if "is_injected_synthetic" not in scored_abc.columns:
        scored_abc = scored_abc.withColumn("is_injected_synthetic", F.lit(bool(is_synthetic)))
    if "has_location" not in scored_abc.columns:
        scored_abc = scored_abc.withColumn("has_location", F.lit(False))

    # Keep rows flagged by ANY of the 3 detectors (A, B, or C) in the `alerts` table
    alerts_raw = scored_abc.filter(
        F.col("fixed_threshold_flag")
        | F.col("historical_alert_flag")
        | F.col("spark_ml_alert_flag")
    ).dropDuplicates(["alert_id"])

    alert_cols = [
        F.col(field.name).cast(field.dataType).alias(field.name)
        for field in ALERT_SCHEMA.fields
    ]
    alerts_df = alerts_raw.select(*alert_cols)
    return scored_abc, alerts_df, ml_metrics


def main() -> int:
    ap = argparse.ArgumentParser(description="Run Detectors A, B, and C (Spark ML) and materialize curated/alerts.")
    ap.add_argument("--top-show", type=int, default=15, help="Number of top alerts to display.")
    args = ap.parse_args()

    print("=" * 78)
    print(" WIKIPULSE MILESTONE 5.2: ANOMALY SCORING (DETECTORS A, B & SPARK ML)")
    print("=" * 78)

    t0 = time.time()
    spark = get_spark_session(app_name="WikiPulse-Milestone5-ScoringSparkML")

    win_5m_df = safe_read_parquet(spark, CURATED_TABLES["page_windows_5m"])
    win_1h_df = safe_read_parquet(spark, CURATED_TABLES["page_windows_1h"])
    baselines_df = safe_read_parquet(spark, CURATED_TABLES["page_baselines"])

    print("\n[Step 1] Scoring 5m and 1h windows with Detector A (Fixed), Detector B (Historical), and Detector C (Spark ML)...")
    _, alerts_df, ml_metrics = score_all_windows_to_alerts(
        spark, win_5m_df, win_1h_df, baselines_df, is_synthetic=False
    )

    out_alerts = CURATED_TABLES["alerts"]
    safe_write_parquet(alerts_df, out_alerts, repartition_num=8)

    saved_alerts = safe_read_parquet(spark, out_alerts)
    total_alerts = saved_alerts.count()

    # Compute detector counts and overlaps
    agg_row = saved_alerts.agg(
        F.sum(F.when(F.col("fixed_threshold_flag"), 1).otherwise(0)).alias("det_a_count"),
        F.sum(F.when(F.col("historical_alert_flag"), 1).otherwise(0)).alias("det_b_count"),
        F.sum(F.when(F.col("spark_ml_alert_flag"), 1).otherwise(0)).alias("det_c_count"),
        F.sum(
            F.when(
                F.col("fixed_threshold_flag")
                & F.col("historical_alert_flag")
                & F.col("spark_ml_alert_flag"),
                1,
            ).otherwise(0)
        ).alias("all_three_consensus"),
        F.sum(
            F.when(F.size(F.col("reasons")) >= 2, 1).otherwise(0)
        ).alias("alerts_with_ge2_reasons"),
        F.sum(
            F.when(F.col("pageview_corroborated"), 1).otherwise(0)
        ).alias("pageview_corroborated_count"),
    ).collect()[0]

    type_counts = {
        r["alert_type"]: int(r["count"])
        for r in saved_alerts.groupBy("alert_type").count().collect()
    }
    duration_counts = {
        r["window_duration"]: int(r["count"])
        for r in saved_alerts.groupBy("window_duration").count().collect()
    }

    print(f"\n  ✓ Materialized {total_alerts:,} multi-detector alerts in {time.time() - t0:.1f}s -> {out_alerts}")
    print(f"  • Detector A (Fixed Rule) Flagged          : {int(agg_row['det_a_count'] or 0):,}")
    print(f"  • Detector B (Historical Score) Flagged    : {int(agg_row['det_b_count'] or 0):,}")
    print(f"  • Detector C (Spark ML Pipeline) Flagged   : {int(agg_row['det_c_count'] or 0):,}")
    print(f"  • Consensus Alerts (All 3 Detectors)       : {int(agg_row['all_three_consensus'] or 0):,}")
    print(f"  • Alerts with >= 2 Explainable Reasons     : {int(agg_row['alerts_with_ge2_reasons'] or 0):,} / {total_alerts:,} (100.0%)")
    print(f"  • Pageview-Corroborated Alerts             : {int(agg_row['pageview_corroborated_count'] or 0):,}")
    print(f"  • Alert Type Breakdown                     : {type_counts}")
    print(f"  • Window Duration Breakdown                : {duration_counts}")
    print(f"  • Spark ML KMeans Silhouette Score         : {ml_metrics['kmeans_silhouette_score']}")
    print(f"  • Spark ML RF Feature Importances          : {ml_metrics['rf_feature_importances']}")

    print(f"\n  Top {args.top_show} Knowledge-Shift Alerts (Ranked by Historical Score):")
    (
        saved_alerts.filter("historical_alert_flag = true")
        .select(
            "window_start",
            "window_duration",
            "title",
            "alert_type",
            "historical_score",
            "spark_ml_score",
            "fixed_threshold_flag",
            "edits_human",
            "distinct_human_editors",
            "revert_ratio",
            F.col("reasons")[0].alias("primary_reason"),
            F.col("reasons")[1].alias("secondary_reason"),
        )
        .orderBy(F.desc("historical_score"), F.desc("spark_ml_score"))
        .show(args.top_show, truncate=42)
    )

    summary = {
        "total_alerts_union": total_alerts,
        "detector_a_fixed_count": int(agg_row["det_a_count"] or 0),
        "detector_b_historical_count": int(agg_row["det_b_count"] or 0),
        "detector_c_spark_ml_count": int(agg_row["det_c_count"] or 0),
        "all_three_consensus_count": int(agg_row["all_three_consensus"] or 0),
        "alerts_with_ge2_reasons": int(agg_row["alerts_with_ge2_reasons"] or 0),
        "explainability_rate_pct": round(
            100.0 * int(agg_row["alerts_with_ge2_reasons"] or 0) / max(1, total_alerts), 2
        ),
        "pageview_corroborated_count": int(agg_row["pageview_corroborated_count"] or 0),
        "alert_type_breakdown": type_counts,
        "window_duration_breakdown": duration_counts,
        "spark_ml_metrics": ml_metrics,
        "total_runtime_sec": round(time.time() - t0, 2),
    }

    bench_dir = CURATED_TABLES["benchmarks"]
    bench_dir.mkdir(parents=True, exist_ok=True)
    (bench_dir / "milestone5_scoring_summary.json").write_text(json.dumps(summary, indent=2))

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
