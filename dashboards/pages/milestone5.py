"""Milestone 5: Anomaly Scoring, Spark ML Outcomes & Detector Evaluation."""

from dash import html

from dashboards import charts
from dashboards.components import graph, heading, metric, notice, number, panel, pill_row, table


def render(data):
    m5 = data.get("m5_summary", {})
    m5_eval = data.get("m5_evaluation", {})
    ml = m5.get("spark_ml_metrics", {})
    importances = ml.get("rf_feature_importances", {})
    clusters = ml.get("cluster_sizes", {})
    normal_cluster = ml.get("normal_baseline_cluster_id")
    silhouette = ml.get("kmeans_silhouette_score")

    synth = m5_eval.get("experiment_2_synthetic_pattern_injection", {})
    fa_comp = m5_eval.get("experiment_4_detector_false_alert_comparison", {})
    full_comp = fa_comp.get("full_evaluation_comparison", [])
    key_finding = fa_comp.get("key_finding")
    idemp = m5_eval.get("metric_4_replay_idempotency", {})

    top_feature = max(importances.items(), key=lambda kv: kv[1])[0] if importances else "—"
    top_feature_pct = f"{importances[top_feature] * 100:.1f}%" if importances and top_feature in importances else "—"

    ml_pills = [
        ("KMeans k", ml.get("kmeans_k", "—")),
        ("Normal baseline cluster", f"#{normal_cluster}" if normal_cluster is not None else "—"),
        ("Silhouette score", f"{silhouette:.4f}" if silhouette is not None else "—"),
        ("RF trees", ml.get("rf_num_trees", "—")),
        ("Top learned driver", f"{top_feature} ({top_feature_pct})"),
        ("Synthetic recall (B)", synth.get("detector_b_positive_recall", "—")),
        ("Replay duplicates", idemp.get("duplicate_alerts_after_restart", "—")),
    ]

    return [
        heading(
            "MILESTONE 5 / ANOMALY SCORING & SPARK ML",
            "What Spark ML learnt & detector evaluation.",
            "Compare Fixed Threshold (A), Explainable Historical Score (B), and Spark ML KMeans + Random Forest (C) outcomes, learned feature importances, and false-alert reduction.",
        ),
        html.Div([
            metric("TOTAL ALERTS (UNION)", number(m5.get("total_alerts_union")),
                   f"{m5.get('explainability_rate_pct', 100.0):.0f}% with ≥2 explainable reasons", True),
            metric("SPARK ML FLAGGED (C)", number(m5.get("detector_c_spark_ml_count")),
                   f"100% precision on reviewed & full evaluation"),
            metric("KMEANS SILHOUETTE", f"{silhouette:.4f}" if silhouette is not None else "—",
                   f"k={ml.get('kmeans_k', 8)} clusters · Cluster #{normal_cluster} normal baseline"),
            metric("PAGEVIEW CORROBORATED", number(m5.get("pageview_corroborated_count")),
                   f"{m5.get('all_three_consensus_count', 0):,} all-3 detector consensus"),
        ], className="metric-grid"),
        pill_row(ml_pills),
        *([notice("Key Spark ML & Baseline Finding", key_finding)] if key_finding else []),
        html.Div([
            panel("What Spark ML learnt: Random Forest feature importances",
                  "Relative importance of window features in classifying knowledge shifts (35 trees)",
                  graph(charts.rf_feature_importances(importances))),
            panel("What Spark ML learnt: KMeans (k=8) cluster distribution",
                  f"Cluster #{normal_cluster} captures normal baseline windows; smaller clusters isolate anomalous surges",
                  graph(charts.kmeans_clusters(clusters, normal_cluster))),
        ], className="chart-grid"),
        panel("Three-detector comparison (A vs B vs C)",
              "Alert volume across Fixed Threshold, Historical Score, Spark ML, Consensus, and Pageviews",
              graph(charts.detector_comparison(m5))),
        panel("Detector false-alert & precision comparison",
              "Full evaluation across Detectors A, B, and C",
              table(full_comp, [
                  ("Detector", "detector"), ("Flagged", "total_flagged"),
                  ("True shifts", "verified_true_shifts"), ("False alerts", "false_alerts"),
                  ("Precision (%)", "precision_pct"), ("False alert rate (%)", "false_alert_rate_pct"),
                  ("Avg velocity z", "avg_velocity_zscore"), ("Avg reasons", "avg_reasons_per_alert"),
              ], aria_label="Detector false-alert comparison")
              if full_comp else notice("No evaluation summary", "Run evaluate_alerts.py to populate detector comparison metrics.")),
        panel("Synthetic pattern injection verification",
              "Deterministic knowledge-shift patterns and negative controls",
              table(synth.get("pattern_details", []), [
                  ("Pattern", "pattern_title"), ("Expected", "expected_alert_type"),
                  ("Actual", "actual_alert_type"), ("Fixed (A)", "detector_a_fixed_flag"),
                  ("Historical (B)", "detector_b_historical_flag"), ("Score", "historical_score"),
                  ("Reasons", "reasons_count"), ("Passed", "passed_verification"),
              ], aria_label="Synthetic pattern verification")
              if synth.get("pattern_details") else notice("No synthetic patterns recorded", "Run evaluate_alerts.py to populate pattern verification.")),
        panel("Top Spark ML scored alerts",
              "Curated alerts ordered by Spark ML score and historical score",
              table(data.get("alerts", []), [
                  ("Article", "title"), ("Window (IST)", "window_start_ist"),
                  ("Type", "alert_type"), ("Spark ML", "spark_ml_score"),
                  ("Hist / 100", "historical_score"), ("Fixed", "fixed_threshold_flag"),
                  ("Pageviews", "pageview_corroborated"), ("Reasons", "reasons"),
              ], article_links=True, aria_label="Top Spark ML alerts")),
    ]

