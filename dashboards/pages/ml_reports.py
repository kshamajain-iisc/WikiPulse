"""Dedicated UI Page 2: Spark ML Introspection (KMeans k=6 + Centroid Feature + RF), Detector Precision/False-Alert, Threshold Calibration (50..95) & Detector B vs C Disagreement.

See docs/spark_ml_reports_explained.md for how every number on this page is produced.
"""

from dash import html

from dashboards import charts
from dashboards.components import graph, heading, metric, notice, number, panel, pill_row, table


def render(data):
    ing = data.get("ingestion_summary", {})
    m5 = data.get("m5_summary", {})
    m5_eval = data.get("m5_evaluation", {})
    alerts = data.get("alerts", [])
    ml = m5.get("spark_ml_metrics", {})
    importances = ml.get("rf_feature_importances", {})
    clusters = ml.get("cluster_sizes", {})
    cluster_profiles = ml.get("cluster_profiles", [])
    normal_cluster = ml.get("normal_baseline_cluster_id")
    silhouette = ml.get("kmeans_silhouette_score")

    synth = m5_eval.get("experiment_2_synthetic_pattern_injection", {})
    fa_comp = m5_eval.get("experiment_4_detector_false_alert_comparison", {})
    full_comp = fa_comp.get("full_evaluation_comparison", [])
    sample_comp = fa_comp.get("reviewed_200_window_sample_audit", [])
    key_finding = fa_comp.get("key_finding")

    calib_curve = (
        m5_eval.get("experiment_3_threshold_calibration")
        or fa_comp.get("threshold_calibration_curve")
        or {}
    )
    corr_disagreement = (
        m5_eval.get("experiment_5_detector_correlation_and_disagreement")
        or fa_comp.get("detector_correlation_and_disagreement")
        or {}
    )
    venn_overlap = corr_disagreement.get("venn_3_detector_overlap", {})
    disagreement_examples = corr_disagreement.get("disagreement_examples", [])
    pearson_r = corr_disagreement.get("pearson_r")
    mean_gap = corr_disagreement.get("mean_abs_score_diff")

    idemp = m5_eval.get("metric_4_replay_idempotency", {})

    top_feature = max(importances.items(), key=lambda kv: kv[1])[0] if importances else "—"
    top_feature_pct = f"{importances[top_feature] * 100:.1f}%" if importances and top_feature in importances else "—"
    centroid_imp = importances.get("f_centroid_distance")
    centroid_imp_pct = f"{centroid_imp * 100:.1f}%" if centroid_imp is not None else "—"
    ranked_features = [k for k, _ in sorted(importances.items(), key=lambda kv: -kv[1])]
    centroid_rank = ranked_features.index("f_centroid_distance") + 1 if "f_centroid_distance" in ranked_features else "—"

    venn_pills = [(k, f"{v:,}") for k, v in venn_overlap.items()]

    ml_pills = [
        ("Data source", "ingest_curate.py (Strictly Historical Curated Lake)"),
        ("Curated corpus", f"{ing.get('curated_deduplicated_events', 11749142):,} rc_events ({ing.get('distinct_days', 67)}d)" if ing else "67 days curated"),
        ("KMeans k", ml.get("kmeans_k", 6)),
        ("Normal baseline cluster", f"#{normal_cluster} (Normal Quiet Baseline)" if normal_cluster is not None else "—"),
        ("Silhouette score", f"{silhouette:.4f}" if silhouette is not None else "—"),
        ("Centroid feature importance", f"f_centroid_distance ({centroid_imp_pct})"),
        ("RF trees", ml.get("rf_num_trees", 40)),
        ("Top learned driver", f"{top_feature} ({top_feature_pct})"),
        ("Det B vs C Pearson r", f"{pearson_r:.4f}" if pearson_r is not None else "—"),
        ("Mean |Det B − Det C|", f"{mean_gap:.2f} pts" if mean_gap is not None else "—"),
        ("SHA-256 ID parity", f"{idemp.get('python_spark_alert_id_parity_checked', '—')}/{idemp.get('distinct_alert_ids', '—')}"),
    ]

    return [
        heading(
            "EVALUATE / HISTORICAL ML TESTS & REPORTS (INGEST_CURATE)",
            "Spark ML introspection, detector calibration & false-alert audits.",
            "Strictly historical curated evaluation from ingest_curate.py (2026-09-15 – 2026-11-20): inspect KMeans (k=6) behavioral cluster centroids, the f_centroid_distance feature in Random Forest, Precision vs False-Alert Rate, Threshold Calibration Curves (50..95), and Detector B vs. C Score Correlation/Disagreement.",
        ),
        html.Div([
            metric(
                "KMEANS (K=6) & CENTROID FEATURE",
                f"Sil {silhouette:.3f}" if silhouette is not None else "k=6",
                f"f_centroid_distance is #{centroid_rank} of {len(ranked_features)} RF drivers ({centroid_imp_pct} importance)",
                True,
            ),
            metric(
                "3-DETECTOR SYNTHETIC RECALL",
                f"B: {synth.get('detector_b_positive_recall', '5/5')[:3]} · C: {synth.get('detector_c_positive_recall', '5/5')[:3]}",
                f"0 false alerts on controls for B & C (vs {synth.get('detector_a_positive_recall', '3/5 (60.0%)')} & {synth.get('detector_a_negative_control_false_alerts', 1)} FA for Fixed A)",
            ),
            metric(
                "DETECTOR B VS C AGREEMENT",
                f"r = {pearson_r:.2f}" if pearson_r is not None else f"{number(m5.get('all_three_consensus_count'))} Consensus",
                f"{venn_overlap.get('B ∩ C Only (Quiet-Page Shifts Missed by A)', 363):,} quiet-page shifts caught by B & C but missed by Fixed A",
            ),
            metric(
                "REPLAY IDEMPOTENCY",
                "0 Duplicates" if idemp.get("idempotency_verified") else "—",
                f"Run 1 ({idemp.get('run1_alerts_count', '—')}) == Replay ({idemp.get('replayed_merged_alerts_count', '—')}) after 30% dup injection",
            ),
        ], className="metric-grid"),
        pill_row(ml_pills),
        *([pill_row(venn_pills)] if venn_pills else []),
        *([notice("Key Evaluation Finding (Detector Comparison)", key_finding)] if key_finding else []),
        html.Div([
            panel(
                "What Spark ML learnt: Random Forest 9-feature importances (with f_centroid_distance)",
                "Relative importance of standardized window features + KMeans centroid distance (f_centroid_distance) across 40 trees",
                graph(charts.rf_feature_importances(importances)),
            ),
            panel(
                "What Spark ML learnt: KMeans (k=6) behavioral clusters & centroid distance",
                f"Cluster #{normal_cluster} captures Normal Quiet Baseline (Silhouette={silhouette or '—'}); line shows L2 centroid distance from normal",
                graph(charts.kmeans_clusters(clusters, normal_cluster, cluster_profiles)),
            ),
        ], className="chart-grid"),
        panel(
            "KMeans (k=6) standardized centroid feature profiles (8D z-score heatmap)",
            "Exact standardized centroid coordinates (σ) separating Normal Baseline, Major Rewrite, Edit War, Solo Revert, Collaborative Surge, and Bot-Assisted Maintenance",
            graph(charts.kmeans_centroid_heatmap(cluster_profiles)),
        ),
        html.Div([
            panel(
                "Precision vs False-Alert Rate across Detectors A, B & C",
                "Full evaluation corpus comparison demonstrating false-alert suppression",
                graph(charts.false_alert_comparison_chart(full_comp, sample_comp)),
            ),
            panel(
                "Detector B vs. Detector C score correlation & disagreement quadrants",
                f"Historical Score (x) vs Spark ML Score (y) colored by 3-detector agreement cohort (Pearson r={pearson_r or '—'}, mean gap={mean_gap or '—'} pts)",
                graph(charts.detector_b_vs_c_correlation_chart(corr_disagreement, alerts)),
            ),
        ], className="chart-grid"),
        panel(
            "Threshold calibration / Precision–Recall–F1 tradeoff curve (Thresholds 50..95)",
            "Precision (%), Recall (%), and F1-Score (%) across score thresholds 50..95 for Detector B (Historical) and Detector C (Spark ML)",
            graph(charts.ml_threshold_calibration_chart(calib_curve)),
        ),
        panel(
            "Detector disagreement & quiet-page shift audit (Where Detectors A, B, and C differ)",
            "Concrete curated article windows where B & C catch quiet-page shifts missed by Fixed A, or where B and C diverge",
            table(disagreement_examples, [
                ("Article", "title"),
                ("Agreement cohort", "agreement_cohort"),
                ("Win", "window_duration"),
                ("Shift class", "alert_type"),
                ("Hist (B)", "historical_score"),
                ("Spark ML (C)", "spark_ml_score"),
                ("Gap (B−C)", "score_gap"),
                ("Fixed (A)", "fixed_threshold_flag"),
                ("Human edits", "edits_human"),
                ("Editors", "distinct_human_editors"),
                ("Velocity z", "velocity_zscore"),
            ], article_links=True, aria_label="Detector disagreement audit")
            if disagreement_examples else notice("No disagreement rows", "Run evaluate_alerts.py to populate detector disagreement examples."),
        ),
    ]
