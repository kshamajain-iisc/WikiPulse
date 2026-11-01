# WikiPulse — Implementation Review: Activities Completed, File Roles & Rationale (Milestones 1, 4 & 5)

**Project:** WikiPulse — Real-Time Detection and Analysis of Wikipedia Knowledge Shifts  
**Course:** DA231o — Data Engineering at Scale (IISc)  
**Date:** November 1, 2026  

---

## 1. Summary of All Activities Completed So Far

### Activity 1 — Project Audit & Architecture Alignment
- Audited the 8-week Proposal ([WikiPulse Proposal PDF](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/WikiPulse%20-%20Real%20Time%20Analysis%20of%20Wikipedia%20Knowledge%20Shifts%20%20-%20Proposal%20%5BTeam-%20QuadCore%5D%20copy.pdf)), existing feasibility reports, Postman collection, and the 46-day collected dataset in `historical/data/`.
- Established a clean separation of concerns:
  - **`historical/`**: Strictly used for raw data collection and storage (`historical/data/`).
  - **`Repo/src/` & `Repo/docs/`**: Canonical home for all project code, schemas, Spark pipelines, benchmarks, and documentation.
- Created the master implementation plan ([wikipulse_milestones_4_5_6_plan.md](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/wikipulse_milestones_4_5_6_plan.md)).

### Activity 2 — Wikimedia API Compliance (`USER_AGENT` Fix) & Collection Completion
- **What was done:** Updated `USER_AGENT` to `"WikiPulse/1.0 (Senthil Raj K; mailto:senthilrajk@iisc.ac.in)"` across all collectors, config files, and Postman variables.
- **Why it was needed:** Wikimedia edge proxies rate-limit requests carrying placeholder User-Agent strings (`your-email@example.com`) with `HTTP 429`. Updating the User-Agent allowed the remaining hours of `2026-10-27` (`16:52..23:59 UTC`) and `2026-10-30` (`11:06..23:59 UTC`) to complete cleanly, giving a full **46-day continuous dataset (`2026-09-15` to `2026-10-30`)**.

### Activity 3 — Milestone 1 Implementation & Verification (*Feasibility & Shared Data Model Complete*)
- **What was done:** Built `Repo/src/config/settings.py`, `Repo/src/models/schemas.py`, `Repo/src/verify_milestone1.py`, and `Repo/docs/milestone1_specs_and_models.md`.
- **Why it was needed:** Milestone 1 requires confirming all external APIs, formalizing the shared data models (`Raw edit`, `Page windows`, `Historical baseline`, `Alerts`, `Graph edge`, `Map point`, `Revert risk`, `Revision diff`), and locking down the 4 Dashboard Specifications before downstream batch/streaming modules are built.

### Activity 4 — Raw Data Hygiene: Overlap Deduplication & Multi-Member Gzip Recovery
- **What was done:** Audited all 46 daily `rc_YYYY-MM-DD.jsonl.gz` files and discovered two data-hygiene issues caused by earlier interrupted collector runs:
  1. **Duplicate Overlap on `2026-09-26` & `2026-09-27`:** An interrupted `2026-09-25..2026-10-01` run wrote events up to `2026-09-27T00:24:27Z` before a fresh `2026-09-26..2026-10-01` run appended to the same files.
  2. **Unterminated Multi-Member Gzip Stream in `rc_2026-10-27.jsonl.gz`:** When the first `2026-10-27` run stopped at `16:52:30Z` without writing a gzip trailer and the resumed run appended a second gzip member at byte offset `12,826,637`, standard `gzip.open()` threw `zlib.error: Error -3 while decompressing data: invalid block type`.
- **How we fixed it:** Implemented `iter_recoverable_gz_lines()` and `deduplicate_raw_gz_file()` in [ingest_curate.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/historical/ingest_curate.py), which scans for `\x1f\x8b\x08` gzip member headers, salvages every valid JSON line across all concatenated members, deduplicates by `rcid`, and atomically rewrites a clean single-stream `.jsonl.gz` file. All **269,017 rows** of `2026-10-27` were recovered without data loss.

### Activity 5 — Milestone 4 Implementation & Execution (*Historical Comparison Enabled*)
- **What was done:** Built and executed all three Milestone 4 pipelines (`ingest_curate.py`, `build_baselines.py`, `rankings_benchmark.py`), resolving macOS Finder `Icon\r` file collisions on Hadoop's `_temporary` directory and Parquet schema inference via `safe_write_parquet()` and `safe_read_parquet()`.
- **Measured Results Achieved:**
  - **14,398,713** raw events processed $\rightarrow$ **8,428,021** valid `edit`/`new`/`log` events stored in partitioned Parquet across **46 days** (**100% acceptance rate**, exceeding the $\ge 99\%$ Proposal target).
  - Per-page historical baselines built for `2026-09-15 .. 2026-10-05` with zero-inclusive daily/hourly moments and Wikimedia Analytics pageview enrichment.
  - **Manual Churn Audit:** Exact match (`spark_exact_match: true`) between pure-Python stdlib and Spark DataFrame on a 2,000-event sample (`net_bytes = +845,023`, `gross_churn = 2,024,277`, `rewriting_bytes = 1,179,254`).
  - **RDD vs. DataFrame Benchmark:** On 7 days (`747,156` mainspace edits), DataFrame ran in **1.34s** vs. RDD in **2.716s** (**2.03x–2.82x speedup**).
  - **Dashboard Query Latency Benchmark:** Raw 1-day `JSONL.gz` top-20 query took **2.154s**, whereas Stored Parquet Rankings took **0.148s** and 46-day Churn Series took **0.153s** (**~14.5x faster**, meeting the $< 2.0\text{s}$ Proposal target).

### Activity 6 — Milestone 5 Implementation, Deep-Dive Audit & End-to-End Retest (*Repeatable Alerts Detected & Evaluated*)
- **What was done:** Built, audited, and executed all three Milestone 5 modules in [`Repo/src/anomaly/`](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/anomaly) (`window_features.py`, `scoring_sparkml.py`, `evaluate_alerts.py`).
- **Deep-Dive Gaps Identified & Resolved:**
  1. **Per-Window Editor Surge Scaling:** Replaced linear `1/288` 5-minute editor division (which clamped pages with $< 57.6$ daily editors to `0.20`) with calibrated session-clustered window scaling (`1/18` for `5m`, `1/6` for `1h`, `0.08` std floor), differentiating quiet (`0.15` editors/day), moderate (`1.2` editors/day), and hyperactive (`48.0` editors/day) pages.
  2. **Additive `feature_contributions` Parity:** Applied low-surge (`velocity_zscore < 2.5`) and bot-only (`edits_human == 0`) dampening directly to individual feature contribution weights so `sum(feature_contributions.values()) == historical_score` on every row.
  3. **`CONFIRMED_NEWS` vs. `EDIT_WAR_CONTROVERSY` Disambiguation:** Refined the secondary edit-war rule to require `revert_ratio >= 0.22` and `rewriting_bytes >= 0.5 * gross_bytes` so high-volume collaborative breaking news with low revert rates (e.g., `RJ Mitte`, `69` edits, `26` editors, `5.8%` reverts) is classified as `CONFIRMED_NEWS` rather than `EDIT_WAR_CONTROVERSY`.
  4. **Detector C (Spark ML) Selectivity Calibration:** Added multi-editor / major-rewrite guardrails (`spark_ml_score >= 68.0`) to eliminate 1-editor minor-edit false triggers, reducing total union alerts from `10,134` to `3,564` high-confidence alerts (`Detector C precision = 99.7%`).
  5. **Reason String Sign Formatting & Bot-Only Explanation:** Fixed negative $z$-score string formatting (`+-0.1` $\rightarrow$ `-0.1`) and added explicit `"Bot-only activity"` explanations when `edits_human == 0`.
  6. **Cross-Language `alert_id` SHA-256 Parity:** Verified `39/39` (100%) byte-for-byte parity between Spark SQL `F.sha2` `alert_id` and Python `compute_deterministic_alert_id()`.

---

## 2. File-by-File Inventory: Role, What It Does & Why It Is Needed

### 2.1 Configuration & Shared Data Models (Milestone 1)

| File | Role | What It Does | Why It Is Needed |
| :--- | :--- | :--- | :--- |
| [Repo/src/config/settings.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/config/settings.py) | Central Configuration, API Registry & Spark Helpers | • Defines paths to `historical/data/` raw and curated Parquet tables.<br/>• Registers all 7 confirmed Wikimedia APIs (`WIKIMEDIA_APIS`) and request builders (`build_analytics_per_page_urls`, `build_compare_params`, `build_page_links_coords_params`, `build_backlinks_params`, `build_liftwing_request`, `build_wikidata_entities_params`).<br/>• Provides `get_spark_session()`, `safe_write_parquet()`, and `safe_read_parquet()`. | • Prevents hardcoded paths or mismatched API parameters across scripts.<br/>• `safe_write_parquet()` stages writes in `/tmp` before moving to `curated/`, preventing macOS Finder `Icon\r` daemons from breaking Hadoop's `_temporary` commit protocol.<br/>• `safe_read_parquet()` applies `pathGlobFilter="*.parquet"` so 0-byte `Icon\r` files never break Parquet schema inference. |
| [Repo/src/config/__init__.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/config/__init__.py) | Package Export | Exports constants, paths, API builders, and Spark helpers from `settings.py`. | Clean module imports (`from src.config import ...`) across all pipeline packages. |
| [Repo/src/models/schemas.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/models/schemas.py) | Shared PySpark Data Models & Normalization | • Defines strict `StructType` schemas for all 8 shared tables: `RAW_EDIT_SCHEMA`, `PAGE_WINDOW_SCHEMA`, `HISTORICAL_BASELINE_SCHEMA`, `ALERT_SCHEMA`, `GRAPH_EDGE_SCHEMA`, `MAP_POINT_SCHEMA`, `REVERT_RISK_SCHEMA`, and `REVISION_DIFF_SCHEMA`.<br/>• Implements `normalize_recentchanges_df()`, `normalize_revisions_df()`, and `compute_deterministic_alert_id()`. | • Enforces a single schema contract across batch, streaming, ML, and dashboard layers (Milestone 1 requirement).<br/>• Unifies differences between `recentchanges` and `allrevisions` (e.g., temporary accounts `~2026-...` mapped to `is_anon`, `newlen - oldlen` mapped to `delta`, and revert tags split into `is_reverted` vs. `is_revert_action`).<br/>• `compute_deterministic_alert_id()` hashes `(pageid, window_start, duration)` to guarantee **zero duplicate alerts** on replay/restart. |
| [Repo/src/models/__init__.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/models/__init__.py) | Package Export | Exports all 8 schemas and normalization functions. | Allows all downstream scripts to import schemas from `src.models`. |
| [Repo/src/verify_milestone1.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/verify_milestone1.py) | Milestone 1 Verification Suite | Instantiates all 8 PySpark schemas, tests all API parameter builders, checks deterministic alert IDs, and validates normalization against real `rc_2026-09-15` and `revisions_2026-09-15` files. | Provides a repeatable, automated smoke test proving Milestone 1's shared data models and API contracts work end-to-end. |

---

### 2.2 Historical Ingestion, Baselines & Rankings (Milestone 4)

| File | Role | What It Does | Why It Is Needed |
| :--- | :--- | :--- | :--- |
| [Repo/src/historical/ingest_curate.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/historical/ingest_curate.py) | Milestone 4.1: Raw Cleanup & Curated Parquet Ingestion | • Recovers multi-member `.jsonl.gz` files and deduplicates raw files in-place (`--clean-raw-overlap`).<br/>• Reads all 46 `rc_*.jsonl.gz` files in Spark, drops `categorize` noise (~41% of raw stream), deduplicates by `rcid`, normalizes to `RAW_EDIT_SCHEMA`, and writes partitioned Parquet to `historical/data/curated/rc_events/day=YYYY-MM-DD/`.<br/>• Writes `curated/benchmarks/ingestion_summary.json`. | • Raw `.jsonl.gz` files are slow to scan repeatedly (~2.15s per day) and contained duplicate overlap on Sept 26–27 plus an unterminated gzip stream on Oct 27.<br/>• Produces the clean, partition-prunable Parquet lake required by Milestones 4, 5, and 6 and verifies the Proposal metric: *$\ge 99\%$ of valid accepted events reach storage*. |
| [Repo/src/historical/build_baselines.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/historical/build_baselines.py) | Milestone 4.2: Per-Page Historical Baselines & Analytics Join | • Computes per-page baseline statistics over the 21-day baseline window (`2026-09-15 .. 2026-10-05`): `avg_daily_edits`, `std_daily_edits`, `avg_hourly_edits`, `std_hourly_edits`, `p95_hourly_edits`, `avg_distinct_editors_daily`, `avg_gross_bytes_daily`, `baseline_revert_ratio`, `baseline_bot_ratio`, and `activity_tier` (`quiet`, `moderate`, `active`, `hyperactive`).<br/>• Enriches top active articles with Wikimedia Analytics daily pageviews and records `last_available_date`, `has_missing_data`, `last_pageview_date`, and `pageview_missing_flag`.<br/>• Writes `historical/data/curated/page_baselines`. | • **Core Project Motivation:** Fixed rules like *"15 edits in 5 minutes"* fail because baseline behavior varies wildly across pages.<br/>• Uses zero-inclusive sufficient statistics ($\sum x / D$ and $\sqrt{\sum x^2 / D - \mu^2}$ across all calendar days/hours) with variance floors so quiet pages have accurate baselines without dividing by zero.<br/>• Satisfies Proposal Feature Metric #8: *Historical charts show the source's last available date and distinguish missing data*. |
| [Repo/src/historical/rankings_benchmark.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/historical/rankings_benchmark.py) | Milestone 4.3: Rankings, Churn/Bot Analytics & Scale Benchmarks | • Builds `curated/churn_bot_summary` (daily & namespace breakdown of Human vs. Temp/Anon vs. Bot edits, Net Growth $\sum \Delta$, Gross Churn $\sum |\Delta|$, and **Rewriting Volume** $\sum |\Delta| - |\sum \Delta|$).<br/>• Builds `curated/rankings_daily` (joining daily article activity with `page_baselines` to compare `baseline_surge_rank` vs. `raw_edit_rank`).<br/>• Runs the 2,000-event pure-Python vs. Spark manual churn audit.<br/>• Runs the 7-day **PySpark RDD vs. DataFrame** aggregation benchmark.<br/>• Runs the **JSONL.gz vs. Stored Parquet** query latency benchmark and writes `curated/benchmarks/milestone4_benchmarks.json`. | • Delivers **Supported Feature #5 (Knowledge Churn & Bot Ratio)**, explicitly solving the "rewriting" gap by measuring $\sum |\Delta| - |\sum \Delta|$ (bytes rewritten that cancel out in net page size).<br/>• Directly satisfies **Proposal Experiment Plan #1, #3, #6** and **Performance/Feature Metrics #6, #7** (manual sample verification, RDD vs. DataFrame comparison, and $<2\text{s}$ dashboard query latency). |

---

### 2.3 Anomaly Detection, Explainable Scoring & Spark ML (Milestone 5)

| File | Role | What It Does | Why It Is Needed |
| :--- | :--- | :--- | :--- |
| [Repo/src/anomaly/window_features.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/anomaly/window_features.py) | Milestone 5.1: 5m & 1h Window Feature Extraction | • Aggregates `curated/rc_events` (`2026-10-06 .. 2026-10-30`) into **5-minute (`5m`)** and **1-hour (`1h`)** page activity windows conforming to `PAGE_WINDOW_SCHEMA`.<br/>• Joins with `curated/page_baselines` to compute `velocity_zscore`, `editor_surge_ratio`, `churn_zscore`, `rewriting_bytes`, and `consensus_factor`.<br/>• Writes `curated/page_windows_5m` (`350,192` rows) and `curated/page_windows_1h` (`189,517` rows). | • Provides the unified window feature layer required by Detectors A, B, and C as well as Dashboard 1 (`Live & Replay Alert Radar`). |
| [Repo/src/anomaly/scoring_sparkml.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/anomaly/scoring_sparkml.py) | Milestone 5.2: Multi-Detector Scoring & Spark ML | • Runs **Detector A (Fixed Threshold)**, **Detector B (Explainable Historical Score `0–100` with $\ge 2$ `reasons` and additive `feature_contributions`)**, and **Detector C (Spark ML `VectorAssembler` $\rightarrow$ `StandardScaler` $\rightarrow$ `KMeans(k=8)` + `RandomForestClassifier(numTrees=35)`)**.<br/>• Joins delayed daily pageviews (`pageview_corroborated`) and classifies alerts into `CONFIRMED_NEWS`, `BREAKING_NEWS_CANDIDATE`, `EDIT_WAR_CONTROVERSY`, and `MAJOR_REWRITE`.<br/>• Writes `curated/alerts` (`3,564` rows) with deterministic SHA-256 `alert_id`. | • Solves the core gap where fixed rules fail on quiet/hyperactive pages and misclassify revert wars as breaking news.<br/>• Satisfies Proposal Feature Metric #1 ($\ge 2$ human-readable reasons per alert) and compares interpretable scoring against Spark ML. |
| [Repo/src/anomaly/evaluate_alerts.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/anomaly/evaluate_alerts.py) | Milestone 5.3: Synthetic Injection, False-Alert & End-to-End Audit | • Injects 5 positive knowledge-shift patterns + 2 negative controls and verifies **100% recall (`5/5`)**, **100% additive contribution parity**, and **0 false triggers**.<br/>• Audits false-alert rates across Detectors A (`68.8%`), B (`10.7%`), and C (`0.3%`).<br/>• Runs replay idempotency verification (`30%` duplicate event injection $\rightarrow$ `0` duplicate alerts) and validates all 7 curated tables through Milestone 5. | • Directly satisfies **Proposal Experiment Plan #2 & #4**, **Feature Metric #1 & #2**, and **Performance Metric #4 & #6**. |

---

### 2.4 Documentation & Planning Files

| File | Role | What It Contains |
| :--- | :--- | :--- |
| [wikipulse_milestones_4_5_6_plan.md](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/wikipulse_milestones_4_5_6_plan.md) | Master Implementation Plan | End-to-end roadmap across Milestones 1, 4, 5, and 6, architecture diagrams (Mermaid + ASCII), directory structure under `Repo/src/`, and verification matrix. |
| [Repo/docs/milestone1_specs_and_models.md](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/docs/milestone1_specs_and_models.md) | Milestone 1 Technical Specification | Confirmed Wikimedia API contracts, ER & ASCII data model diagrams, source-to-target field mapping table, and detailed specs for the 4 WikiPulse Dashboards. |
| [Repo/docs/implementation_review_milestones_1_and_4.md](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/docs/implementation_review_milestones_1_and_4.md) | This Document | Comprehensive review of all activities completed so far, file-by-file what/why breakdown, and measured benchmark results. |

---

## 3. Curated Datasets & Benchmark Artifacts Generated (`historical/data/curated/`)

| Curated Path | Format | Schema / Contents | Key Measured Stats |
| :--- | :--- | :--- | :--- |
| `curated/rc_events/` | Partitioned Parquet (`day=YYYY-MM-DD`) | `RAW_EDIT_SCHEMA` (29 cols) | **8,428,021** deduplicated `edit`/`new`/`log` rows across **46 days** (`1,011.43 MB`) |
| `curated/page_baselines/` | Parquet | `HISTORICAL_BASELINE_SCHEMA` (25 cols) | Per-page daily/hourly moments, `activity_tier`, and Analytics pageview baselines (`2026-09-15 .. 2026-10-05`) |
| `curated/churn_bot_summary/` | Parquet | Daily $\times$ Namespace rollup (17 cols) | Human registered, Temp/Anon (`~2026-...`), and Bot edits; `bytes_added`, `bytes_deleted`, `net_bytes`, `gross_churn_bytes`, `rewriting_bytes` |
| `curated/rankings_daily/` | Parquet | Daily top articles with baseline comparison (24 cols) | **14,870** rows ranking articles by `raw_edit_rank` and `baseline_surge_rank` (`daily_velocity_zscore`) |
| `curated/page_windows_5m/` | Parquet | `PAGE_WINDOW_SCHEMA` (23 cols) | **350,192** 5-minute windows (`252,724` mainspace `ns=0`) across `2026-10-06 .. 2026-10-30` |
| `curated/page_windows_1h/` | Parquet | `PAGE_WINDOW_SCHEMA` (23 cols) | **189,517** 1-hour windows (`126,328` mainspace `ns=0`) across `2026-10-06 .. 2026-10-30` |
| `curated/alerts/` | Parquet | `ALERT_SCHEMA` (26 cols) | **3,564** multi-detector alerts (**100%** with $\ge 2$ reasons; query latency **0.088s**) |
| `curated/benchmarks/ingestion_summary.json` | JSON | Ingestion completeness & storage size | `14,398,713` raw $\rightarrow$ `8,428,021` accepted (**100% valid retention**); `977.74 MB` JSONL.gz vs `1,011.43 MB` Parquet |
| `curated/benchmarks/milestone4_benchmarks.json` | JSON | Manual audit, RDD vs. DF, and query latency | • Manual audit: **100% exact match**<br/>• DF (`1.34s`) vs. RDD (`2.716s`): **2.03x–2.82x speedup**<br/>• Stored Parquet query time: **0.148s – 0.153s** ($< 2.0\text{s}$ target met) |
| `curated/benchmarks/milestone5_scoring_summary.json` | JSON | Multi-detector alert counts & Spark ML metrics | • Detector A (`885`), Detector B (`2,583`), Detector C (`2,083`), Consensus (`509`)<br/>• KMeans Silhouette = **0.4229**, RF top feature = `f_distinct_editors` (`0.3438`) |
| `curated/benchmarks/milestone5_evaluation.json` | JSON | Synthetic injection, false-alert & replay idempotency | • Synthetic recall: **5/5 (100%)** for Detector B vs **3/5 (60%)** for Detector A<br/>• False-alert rate reduced from **68.8%** (Detector A) to **10.7%** (Detector B) and **0.3%** (Detector C)<br/>• Replay idempotency: **0 duplicate alerts** (`39/39` hash parity) |

---

## 4. Quick Reference: Commands to Run Milestones 1, 4 & 5

```bash
# 1. Verify Milestone 1 APIs, Shared Schemas & Normalization
.venv/bin/python3 Repo/src/verify_milestone1.py

# 2. Milestone 4.1: Ingest & Curate 46-Day Partitioned Parquet Lake
.venv/bin/python3 Repo/src/historical/ingest_curate.py

# 3. Milestone 4.2: Build Per-Page Historical Baselines & Pageview Enrichment
.venv/bin/python3 Repo/src/historical/build_baselines.py

# 4. Milestone 4.3: Build Historical Rankings, Churn/Bot Rollups & Run Benchmarks
.venv/bin/python3 Repo/src/historical/rankings_benchmark.py

# 5. Milestone 5.1: Build 5-Minute & 1-Hour Page Window Features
.venv/bin/python3 Repo/src/anomaly/window_features.py

# 6. Milestone 5.2: Run Detectors A, B & Spark ML (Detector C) -> curated/alerts
.venv/bin/python3 Repo/src/anomaly/scoring_sparkml.py

# 7. Milestone 5.3: Run Synthetic Injection, False-Alert & End-to-End Lake Audit
.venv/bin/python3 Repo/src/anomaly/evaluate_alerts.py
```

---

## 5. Next Steps: Milestone 6

With **Milestones 1, 4, and 5** complete and re-verified end-to-end, the remaining milestone is:
1. **Milestone 6 (`Repo/src/enrichment/` & `Repo/src/dashboard/`):**
   - `graph_and_diffs.py` (MediaWiki `links`/`backlinks` spillover graph + `action=compare` diffs with $\ge 90\%$ usable check)
   - `geospatial_map.py` (Wikipedia coordinates + Wikidata `P625`/`P276`/`P17` fallback with source & quality tags; no editor IPs)
   - `revert_risk_monitor.py` (Lift Wing Revert Risk ML scoring + heuristic fallback $\ge 95\%$ check, matched against actual later reverts across thresholds)
   - `dashboard/app.py` (End-to-end investigation dashboard over the curated Parquet tables)

