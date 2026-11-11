# WikiPulse — Implementation Review: Activities Completed, File Roles & Rationale (Milestones 1 through 8)

**Project:** WikiPulse — Real-Time Detection and Analysis of Wikipedia Knowledge Shifts  
**Course:** DA231o — Data Engineering at Scale (IISc)  
**Date:** November 9, 2026  

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
  - Per-page historical baselines built for `2026-09-15 .. 2026-10-31` (with `2026-11-01 .. 2026-11-08` reserved for evaluation) with zero-inclusive daily/hourly moments and Wikimedia Analytics pageview enrichment in `Repo/data/analytics/`.
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

### Activity 7 — Milestone 6 Implementation, Contextual Enrichment & End-to-End Dashboard Verification (*Investigation Flow Complete*)
- **What was done:** Built, executed, and verified all four Milestone 6 components in [`Repo/src/enrichment/`](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/enrichment) (`graph_and_diffs.py`, `geospatial_map.py`, `revert_risk_monitor.py`) and [`Repo/src/dashboard/`](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/dashboard) (`app.py`), and bridged the Phase 2/3 Dash UI (`Repo/dashboards/` and `Repo/src/dashboard_queries.py`) with the 11 curated Parquet tables in `historical/data/curated/`.
- **Measured Results Achieved:**
  1. **Link Graph Spillover & Revision Diffs (`graph_and_diffs.py`):**
     - Generated **607** directed link/backlink edges (`212` outlinks, `395` backlinks) across **36** top alerted articles, identifying **195 co-edited spillover edges (`32.13%`)** active during or within $+1\text{h}$ of the source alert window.
     - Fetched and parsed **100** revision pairs via MediaWiki `action=compare` with per-page diversification (`_rn <= 3`), achieving **90.0% live HTML diff extraction** and **100.0% usable change data** (with metadata fallback on 10 suppressed/deleted revisions), exceeding the Proposal's $\ge 90\%$ target.
  2. **Article-Subject Geospatial Mapping (`geospatial_map.py`):**
     - Evaluated **160** top alerts (`159` unique articles) using a 2-tier article-subject resolver (Tier 1: MediaWiki `prop=coordinates|pageprops`; Tier 2: Wikidata `wbgetentities` traversing `P625`, `P276`, `P131`, `P159`, `P17`, `P27`, `P495`), strictly avoiding editor IP geolocation.
     - Resolved **121 / 160 (`75.62%`)** alerts to map coordinates (`29` `wikipedia_coord`, `5` `wikidata_p625`, `87` `wikidata_p276_p17`; `34` `exact_point`, `16` `related_entity_point`, `71` `country_centroid`) while preserving `39` (`24.38%`) abstract/conceptual alerts as `unlocated` (`has_location=False`) without dropping them. Updated `702` alert windows in `curated/alerts` with `has_location=True`.
  3. **Lift Wing Revert Risk Scoring & 48h Revert Evaluation (`revert_risk_monitor.py`):**
     - Pre-filtered `5,170,879` mainspace edits down to `1,322,673` heuristic candidates (**74.42% routine edits avoided**).
     - Scored **120** candidate revisions (`60` reverted within 48h, `60` non-reverted controls): **111 / 120 (`92.5%`)** live Wikimedia Lift Wing ML scores + **9 / 120 (`7.5%`)** local heuristic fallback = **100.0% valid score coverage** (exceeding the $\ge 95\%$ target).
     - **Threshold Calibration (Proposal Experiment #5):** Across $\tau \in \{0.20, 0.35, 0.50, 0.65, 0.80, 0.90\}$, calibrating `composite_risk` at $\tau = 0.65$ improved **F1 from `68.57%` (at static `0.80`) to `76.92%`** (`Precision = 71.43%`, `Recall = 83.33%`), while standalone Lift Wing `ml_revert_risk` peaked at $\tau = 0.80$ with **F1 = `76.64%`**.
     - Measured 48-hour Revert MTTR: **median `66.5s`** (mean `1388.9s`).
  4. **End-to-End Dashboard Integration & Latency Verification (`Repo/src/dashboard/app.py` & `Repo/dashboards/`):**
     - Verified all **11 curated Parquet tables** (`all_11_tables_valid = True`), all **4 Proposal Dashboard View queries under 2.0s** (`View 1: 0.1193s–0.6677s`, `View 2: 0.7136s–0.8894s`, `View 3: 0.1815s–0.2698s`, `View 4: 0.0040s–0.0064s`; max = **`0.8894s`**), all **5 HTTP routes (`200 OK`)**, and all **45 unit tests in `Repo/tests/` (`OK`)**.

### Activity 8 — Milestone 7 Implementation: Scaling, Shuffle, Load & Failure-Recovery Benchmarks (*Engineering Scale Results Finalized*)
- **What was done:** Built and executed [`Repo/src/benchmarks/scaling_experiments.py`](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/benchmarks/scaling_experiments.py) and [`Repo/src/benchmarks/load_and_recovery_benchmark.py`](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/benchmarks/load_and_recovery_benchmark.py), and upgraded the Engineering Dashboard (`/system`) in [`Repo/dashboards/data_service.py`](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/dashboards/data_service.py), [`Repo/dashboards/charts.py`](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/dashboards/charts.py), and [`Repo/dashboards/pages/system.py`](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/dashboards/pages/system.py).
- **Measured Results Achieved:**
  1. **Strong Scaling (`local[1..6]` on 14-day slice, `2,472,723` events):** Speedup reached **`1.69x` at 4 cores** (`18.97s` $\rightarrow$ `11.21s`, shuffle `280.74 MB`, peak memory `1,045.7 MB`).
  2. **Weak Scaling (`4 days/core`, `4d@1c` $\rightarrow$ `24d@6c`):** Weak-scaling efficiency maintained at **`0.93`** on 2 cores (`199,498 rows/s`) and **`0.61`** on 4 cores (`249,732 rows/s`).
  3. **Shuffle Partition Sweep (`4, 8, 16, 32, 64, 128` partitions):** Empirically proved **`16` partitions (`10.00s`, `17.97 MB/partition`)** is the global optimum on our 6-core / 8 GB unified memory host (`4` parts = `11.97s`; `128` parts = `18.05s`).
  4. **Input-Size Scaling (`1d` $\rightarrow$ `46d`):** Processed the full 46-day lake (`8,428,021` events $\rightarrow$ `443,663` windows) in **`38.09s`** at **`221,295 rows/sec`** (`shuffle = 861.27 MB`, `peak_memory = 1,105.0 MB`, `input_storage = 1,076.72 MB`, `output_storage = 35.22 MB`).
  5. **Streaming Load & Closed 5-Minute Window Latency (`1x`, `10x`, `50x`, `100x`):** Processing capacity (`2,266–8,989 rows/s`) exceeded input rate on every tier, with closed 5-minute windows finalizing in **`0.355s – 9.454s` (`< 60s` Proposal target)**.
  6. **Controlled Stop/Restart & Backlog Recovery (`2026-10-15`, `182,521` unique events):** After stopping the streaming processor and accumulating **`123,537` backlog events** (including `19,379` duplicate re-sent events), restarting from the Spark checkpoint drained the backlog to **`0` in `13.74s–18.65s`** with **`0` duplicate alerts** and **100% parity (`39/39` alerts)** against an uninterrupted reference run.

### Activity 9 — Milestone 8 Implementation: Turnkey Backup Demo, Final Report & Presentation Package (*Reproducible System & Presentation Ready*)
- **What was done:** Built [`Repo/src/demo/backup_demo.py`](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/demo/backup_demo.py), authored [`Repo/docs/final_project_report.md`](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/docs/final_project_report.md) and [`Repo/docs/presentation_and_demo_guide.md`](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/docs/presentation_and_demo_guide.md), updated [`Repo/README.md`](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/README.md), and added automated regression tests in [`Repo/tests/test_milestone7_8.py`](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/tests/test_milestone7_8.py).

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
| [Repo/src/historical/build_baselines.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/historical/build_baselines.py) | Milestone 4.2: Per-Page Historical Baselines & Analytics Join | • Computes per-page baseline statistics over the baseline window (`2026-09-15 .. 2026-10-31`): `avg_daily_edits`, `std_daily_edits`, `avg_hourly_edits`, `std_hourly_edits`, `p95_hourly_edits`, `avg_distinct_editors_daily`, `avg_gross_bytes_daily`, `baseline_revert_ratio`, `baseline_bot_ratio`, and `activity_tier` (`quiet`, `moderate`, `active`, `hyperactive`).<br/>• Populates `Repo/data/analytics/` and enriches top active articles with Wikimedia Analytics daily pageviews, recording `last_available_date`, `has_missing_data`, `last_pageview_date`, and `pageview_missing_flag`.<br/>• Writes `curated/page_baselines`. | • **Core Project Motivation:** Fixed rules like *"15 edits in 5 minutes"* fail because baseline behavior varies wildly across pages.<br/>• Uses zero-inclusive sufficient statistics ($\sum x / D$ and $\sqrt{\sum x^2 / D - \mu^2}$ across all calendar days/hours) with variance floors so quiet pages have accurate baselines without dividing by zero.<br/>• Satisfies Proposal Feature Metric #8: *Historical charts show the source's last available date and distinguish missing data*. |
| [Repo/src/historical/rankings_benchmark.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/historical/rankings_benchmark.py) | Milestone 4.3: Rankings, Churn/Bot Analytics & Scale Benchmarks | • Builds `curated/churn_bot_summary` (daily & namespace breakdown of Human vs. Temp/Anon vs. Bot edits, Net Growth $\sum \Delta$, Gross Churn $\sum |\Delta|$, and **Rewriting Volume** $\sum |\Delta| - |\sum \Delta|$).<br/>• Builds `curated/rankings_daily` (joining daily article activity with `page_baselines` to compare `baseline_surge_rank` vs. `raw_edit_rank`).<br/>• Runs the 2,000-event pure-Python vs. Spark manual churn audit.<br/>• Runs the 7-day **PySpark RDD vs. DataFrame** aggregation benchmark.<br/>• Runs the **JSONL.gz vs. Stored Parquet** query latency benchmark and writes `curated/benchmarks/milestone4_benchmarks.json`. | • Delivers **Supported Feature #5 (Knowledge Churn & Bot Ratio)**, explicitly solving the "rewriting" gap by measuring $\sum |\Delta| - |\sum \Delta|$ (bytes rewritten that cancel out in net page size).<br/>• Directly satisfies **Proposal Experiment Plan #1, #3, #6** and **Performance/Feature Metrics #6, #7** (manual sample verification, RDD vs. DataFrame comparison, and $<2\text{s}$ dashboard query latency). |

---

### 2.3 Anomaly Detection, Explainable Scoring & Spark ML (Milestone 5)

| File | Role | What It Does | Why It Is Needed |
| :--- | :--- | :--- | :--- |
| [Repo/src/anomaly/window_features.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/anomaly/window_features.py) | Milestone 5.1: 5m & 1h Window Feature Extraction | • Aggregates `curated/rc_events` over the evaluation period (`2026-11-01 .. 2026-11-08`, or `--all-days`) into **5-minute (`5m`)** and **1-hour (`1h`)** page activity windows conforming to `PAGE_WINDOW_SCHEMA`.<br/>• Joins with `curated/page_baselines` to compute `velocity_zscore`, `editor_surge_ratio`, `churn_zscore`, `rewriting_bytes`, and `consensus_factor`.<br/>• Writes `curated/page_windows_5m` and `curated/page_windows_1h`. | • Provides the unified window feature layer required by Detectors A, B, and C as well as Dashboard 1 (`Live & Replay Alert Radar`). |
| [Repo/src/anomaly/scoring_sparkml.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/anomaly/scoring_sparkml.py) | Milestone 5.2: Multi-Detector Scoring & Spark ML | • Runs **Detector A (Fixed Threshold)**, **Detector B (Explainable Historical Score `0–100` with $\ge 2$ `reasons` and additive `feature_contributions`)**, and **Detector C (Spark ML `VectorAssembler` $\rightarrow$ `StandardScaler` $\rightarrow$ `KMeans(k=8)` + `RandomForestClassifier(numTrees=35)`)**.<br/>• Joins delayed daily pageviews (`pageview_corroborated`) and classifies alerts into `CONFIRMED_NEWS`, `BREAKING_NEWS_CANDIDATE`, `EDIT_WAR_CONTROVERSY`, and `MAJOR_REWRITE`.<br/>• Writes `curated/alerts` (`3,564` rows) with deterministic SHA-256 `alert_id`. | • Solves the core gap where fixed rules fail on quiet/hyperactive pages and misclassify revert wars as breaking news.<br/>• Satisfies Proposal Feature Metric #1 ($\ge 2$ human-readable reasons per alert) and compares interpretable scoring against Spark ML. |
| [Repo/src/anomaly/evaluate_alerts.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/anomaly/evaluate_alerts.py) | Milestone 5.3: Synthetic Injection, False-Alert & End-to-End Audit | • Injects 5 positive knowledge-shift patterns + 2 negative controls and verifies **100% recall (`5/5`)**, **100% additive contribution parity**, and **0 false triggers**.<br/>• Audits false-alert rates across Detectors A (`68.8%`), B (`10.7%`), and C (`0.3%`).<br/>• Runs replay idempotency verification (`30%` duplicate event injection $\rightarrow$ `0` duplicate alerts) and validates all 7 curated tables through Milestone 5. | • Directly satisfies **Proposal Experiment Plan #2 & #4**, **Feature Metric #1 & #2**, and **Performance Metric #4 & #6**. |

---

### 2.4 Contextual Enrichment & End-to-End Dashboard (Milestone 6)

| File | Role | What It Does | Why It Is Needed |
| :--- | :--- | :--- | :--- |
| [Repo/src/enrichment/graph_and_diffs.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/enrichment/graph_and_diffs.py) | Milestone 6.1: Link Graph Spillover & Revision Diffs | • Queries MediaWiki `prop=links` and `list=backlinks` for top alerted articles and joins against `page_windows_5m`/`1h` to detect co-edited neighbor spillover (`co_edited_in_window`, `target_edits_in_window`, `target_distinct_editors`), writing `curated/graph_edges` (`GRAPH_EDGE_SCHEMA`).<br/>• Queries MediaWiki `action=compare` for candidate revisions on alerted articles, strips HTML diff tables into clean plain-text `added_text_snippet` and `deleted_text_snippet` (`DiffHTMLTableParser`), and writes `curated/revision_diffs` (`REVISION_DIFF_SCHEMA`). | • Delivers **Supported Feature #3 (Related-Article Graph)** and **Supported Feature #2 (Change Summary & Revision Diffs)**.<br/>• Verifies **Proposal Feature Metric #3**: $\ge 90\%$ of compared revision pairs return usable change data (**100.0%** achieved). |
| [Repo/src/enrichment/geospatial_map.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/enrichment/geospatial_map.py) | Milestone 6.2: Article-Subject Geospatial Resolver | • Resolves article-subject coordinates using Tier-1 Wikipedia `prop=coordinates|pageprops` and Tier-2 Wikidata `wbgetentities` (`P625`, `P276`, `P131`, `P159`, `P17`, `P27`, `P495`), tagging `location_source` and `location_quality` (`exact_point`, `related_entity_point`, `country_centroid`, `unlocated`).<br/>• Writes `curated/map_points` (`MAP_POINT_SCHEMA`) and updates `has_location` on `curated/alerts`. | • Delivers **Supported Feature #4 (Article-Subject Map)** while strictly enforcing **Proposal Feature Metric #4**: maps article-subject locations (never editor IPs), records source/quality tags, and keeps unlocated alerts valid. |
| [Repo/src/enrichment/revert_risk_monitor.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/enrichment/revert_risk_monitor.py) | Milestone 6.3: Lift Wing Revert Risk & 48h Revert Monitor | • Pre-filters routine edits (**74.42% avoided**), scores candidate revisions via Wikimedia Lift Wing (`revertrisk-language-agnostic:predict`) with local heuristic fallback, and matches scored edits against 48h delayed revert outcomes (`time_to_revert_sec`, `reverter_is_bot`).<br/>• Evaluates Precision/Recall/F1 across thresholds $\tau \in \{0.20..0.90\}$ vs. static `0.80` and writes `curated/revert_risk_eval` (`REVERT_RISK_SCHEMA`). | • Delivers **Supported Feature #6 (Revert Risk & Vandalism Monitor)**, **Proposal Feature Metric #5** ($\ge 95\%$ valid probability or fallback score; **100.0%** achieved), and **Proposal Experiment #5** (threshold calibration). |
| [Repo/src/dashboard/app.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/dashboard/app.py) | Milestone 6.4: End-to-End Dashboard Benchmark & Entrypoint | • Validates all 11 curated Parquet tables against their schemas, benchmarks all 4 Proposal Dashboard View queries under `< 2.0s`, smoke-tests all 5 Dash HTTP routes (`/`, `/investigation`, `/historical`, `/relationships`, `/system`), and serves the interactive Dash + Plotly app. | • Delivers the unified end-to-end investigation workflow (**Dashboard 1 $\rightarrow$ Dashboard 2 $\rightarrow$ Dashboard 3 $\rightarrow$ Dashboard 4**) and verifies **Proposal Performance Metric #6** (`< 2.0s` query latency). |
| [Repo/src/dashboard_queries.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/dashboard_queries.py) & [Repo/dashboards/data_service.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/dashboards/data_service.py) | Phase 2/3 $\leftrightarrow$ Phase 4/5/6 Query Bridge | • Bridges both `"5m"`/`"1h"` and `"5 minutes"`/`"1 hour"` durations, decomposes `feature_contributions` map into `activity_points`, `editor_points`, `content_points`, `revert_points`, and uses pre-aggregated `churn_bot_summary` & `rankings_daily` tables for sub-second `/historical` queries. | • Allows the Dash UI built in Phase 2/3 to seamlessly query both live streaming tables and the 46-day Milestone 4/5/6 curated lake in `< 1.1s`. |
| [Repo/tests/test_enrichment.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/tests/test_enrichment.py) | Milestone 6 Unit Tests | • Tests HTML diff parsing (`parse_diff_html`), Wikidata coordinate/entity claim extraction, heuristic revert-risk & threshold calibration, and `DashboardQueries` compatibility with Milestone 5 `ALERT_SCHEMA`. | • Ensures deterministic unit test coverage across all Milestone 6 enrichment parsers and dashboard adapters. |

---

### 2.5 Engineering Scale, Load & Failure Recovery (Milestone 7) & Backup Demo (Milestone 8)

| File | Role | What It Does | Why It Is Needed |
| :--- | :--- | :--- | :--- |
| [Repo/src/benchmarks/scaling_experiments.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/benchmarks/scaling_experiments.py) | Milestone 7.1: Strong/Weak Scaling, Shuffle & Input-Size Suite | • Runs Strong Scaling (`local[1..6]`), Weak Scaling (`4d/core`), Shuffle Partition Sweep (`4..128`), Input-Size Scaling (`1d..46d`), and RDD vs. DataFrame telemetry.<br/>• Instruments runtime, shuffle write/read MB, peak JVM+Python RSS MB, and input/output Parquet storage MB. | • Directly satisfies **Proposal Experiment Plan #3 & #4** and **Performance Metric #5** (*Runtime, shuffle, memory, and storage size are reported for every scale experiment*). |
| [Repo/src/benchmarks/load_and_recovery_benchmark.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/benchmarks/load_and_recovery_benchmark.py) | Milestone 7.2: Multi-Speed Load, Window Latency & Stop/Restart Recovery | • Evaluates `1x`, `10x`, `50x`, `100x` replay loads and closed 5m window completion latency (`< 60s`).<br/>• Executes controlled stop, outage backlog + `35%` duplicate injection, and checkpoint restart recovery (`backlog -> 0`, `0` duplicate alerts). | • Directly satisfies **Proposal Experiment Plan #5** and **Performance Metrics #2, #3, #4**. |
| [Repo/src/demo/backup_demo.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/demo/backup_demo.py) | Milestone 8.1: 1-Command Reproducible Backup Demo & Case-Study Tour | • Verifies all 11 curated tables, all 11 benchmark JSONs, and all 5 Dash HTTP routes.<br/>• Extracts and prints 4 distinct real Wikipedia case studies (`Navi Pillay`, `Katie Zacharia`, `Hurricane Isaias (2026)`, `Mike Ditka`) across all 4 alert classes.<br/>• Pre-warms caches and serves the offline-ready Dash UI. | • Ensures a resilient, reproducible live presentation with zero dependency on external Wi-Fi, Wikimedia APIs, or Docker containers. |
| [Repo/tests/test_milestone7_8.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/tests/test_milestone7_8.py) | Milestone 7 & 8 Unit & Integration Tests | • Tests scaling and load charts, `DashboardDataService.system_health()` key mapping, `/system` rendering, and Milestone 7/8 benchmark contracts. | • Brings the automated unit test suite to **45 passing tests (`100% OK`)**. |

---

### 2.6 Documentation & Planning Files

| File | Role | What It Contains |
| :--- | :--- | :--- |
| [wikipulse_milestones_4_5_6_plan.md](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/wikipulse_milestones_4_5_6_plan.md) & [wikipulse_milestones_7_8_plan.md](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/wikipulse_milestones_7_8_plan.md) | Master Implementation Plans | End-to-end roadmap across Milestones 1–8, architecture diagrams, directory structure under `Repo/src/`, and verification matrix. |
| [Repo/docs/milestone1_specs_and_models.md](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/docs/milestone1_specs_and_models.md) | Milestone 1 Technical Specification | Confirmed Wikimedia API contracts, ER & ASCII data model diagrams, source-to-target field mapping table, and detailed specs for the 4 WikiPulse Dashboards. |
| [Repo/docs/final_project_report.md](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/docs/final_project_report.md) | Milestone 8.2: Final Project Report | Complete course report covering problem statement, architecture, data hygiene, algorithms, and full evaluation across all 6 Proposal Experiments and 14 Metrics. |
| [Repo/docs/presentation_and_demo_guide.md](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/docs/presentation_and_demo_guide.md) | Milestone 8.3: Presentation & Demo Guide | 12-slide presentation deck outline and step-by-step 5-minute live/backup demo script. |
| [Repo/docs/implementation_review_milestones_1_and_4.md](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/docs/implementation_review_milestones_1_and_4.md) | This Document | Comprehensive review of all activities completed across Milestones 1 through 8, file-by-file what/why breakdown, and measured benchmark results. |

---

## 3. Curated Datasets & Benchmark Artifacts Generated (`historical/data/curated/`)

| Curated Path | Format | Schema / Contents | Key Measured Stats |
| :--- | :--- | :--- | :--- |
| `curated/rc_events/` | Partitioned Parquet (`day=YYYY-MM-DD`) | `RAW_EDIT_SCHEMA` (29 cols) | **10,190,119** deduplicated `edit`/`new`/`log` rows across **55 days** (`1,220.96 MB`; `8,428,021` rows over the 46-day benchmark slice) |
| `curated/page_baselines/` | Parquet | `HISTORICAL_BASELINE_SCHEMA` (25 cols) | Per-page daily/hourly moments, `activity_tier`, and Analytics pageview baselines (`2026-09-15 .. 2026-10-31`) |
| `curated/churn_bot_summary/` | Parquet | Daily $\times$ Namespace rollup (17 cols) | Human registered, Temp/Anon (`~2026-...`), and Bot edits; `bytes_added`, `bytes_deleted`, `net_bytes`, `gross_churn_bytes`, `rewriting_bytes` |
| `curated/rankings_daily/` | Parquet | Daily top articles with baseline comparison (24 cols) | **14,870** rows ranking articles by `raw_edit_rank` and `baseline_surge_rank` (`daily_velocity_zscore`) |
| `curated/page_windows_5m/` | Parquet | `PAGE_WINDOW_SCHEMA` (23 cols) | 5-minute windows across the evaluation period (`2026-11-01 .. 2026-11-08`, or all days with `--all-days`) |
| `curated/page_windows_1h/` | Parquet | `PAGE_WINDOW_SCHEMA` (23 cols) | 1-hour windows across the evaluation period (`2026-11-01 .. 2026-11-08`, or all days with `--all-days`) |
| `curated/alerts/` | Parquet | `ALERT_SCHEMA` (26 cols) | **3,564** multi-detector alerts (**100%** with $\ge 2$ reasons; `702` alert windows enriched with `has_location=True`; query latency **0.088s**) |
| `curated/graph_edges/` | Parquet | `GRAPH_EDGE_SCHEMA` (12 cols) | **607** directed edges (`212` outlinks, `395` backlinks; **195 co-edited spillover edges**) across `36` alerted articles |
| `curated/revision_diffs/` | Parquet | `REVISION_DIFF_SCHEMA` (16 cols) | **100** compared revision pairs (**90.0%** live HTML diff + **10.0%** metadata fallback = **100.0% usable**, exceeding $\ge 90\%$ target) |
| `curated/map_points/` | Parquet | `MAP_POINT_SCHEMA` (13 cols) | **159** unique alerted articles (**75.62%** resolved to coordinates via Wikipedia/Wikidata; **24.38%** preserved as `unlocated`) |
| `curated/revert_risk_eval/` | Parquet | `REVERT_RISK_SCHEMA` (21 cols) | **120** scored revisions (**92.5%** Lift Wing ML + **7.5%** heuristic fallback = **100.0% coverage**, exceeding $\ge 95\%$ target) |
| `curated/benchmarks/ingestion_summary.json` | JSON | Ingestion completeness & storage size | `14,398,713` raw $\rightarrow$ `8,428,021` accepted (**100% valid retention**); `977.74 MB` JSONL.gz vs `1,011.43 MB` Parquet |
| `curated/benchmarks/milestone4_benchmarks.json` | JSON | Manual audit, RDD vs. DF, and query latency | • Manual audit: **100% exact match**<br/>• DF (`1.34s`) vs. RDD (`2.716s`): **2.03x–2.82x speedup**<br/>• Stored Parquet query time: **0.148s – 0.153s** ($< 2.0\text{s}$ target met) |
| `curated/benchmarks/milestone5_scoring_summary.json` | JSON | Multi-detector alert counts & Spark ML metrics | • Detector A (`885`), Detector B (`2,583`), Detector C (`2,083`), Consensus (`509`)<br/>• KMeans Silhouette = **0.4229**, RF top feature = `f_distinct_editors` (`0.3438`) |
| `curated/benchmarks/milestone5_evaluation.json` | JSON | Synthetic injection, false-alert & replay idempotency | • Synthetic recall: **5/5 (100%)** for Detector B vs **3/5 (60%)** for Detector A<br/>• False-alert rate reduced from **68.8%** (Detector A) to **10.7%** (Detector B) and **0.3%** (Detector C)<br/>• Replay idempotency: **0 duplicate alerts** (`39/39` hash parity) |
| `curated/benchmarks/milestone6_graph_diffs_summary.json` | JSON | Link graph spillover & revision diff coverage | • `607` graph edges (`195` co-edited spillover edges, `32.13%`)<br/>• `100/100` (`100.0%`) usable diffs (`90.0%` live Compare API) |
| `curated/benchmarks/milestone6_geospatial_summary.json` | JSON | Article-subject geospatial resolution summary | • `121/160` (`75.62%`) located (`29` Wikipedia, `5` Wikidata P625, `87` Wikidata P276/P17)<br/>• `39/160` (`24.38%`) unlocated alerts preserved valid; `0` editor IPs used |
| `curated/benchmarks/milestone6_revert_risk_summary.json` | JSON | Lift Wing coverage, threshold calibration & MTTR | • `74.42%` routine edits pre-filtered; `100.0%` valid score coverage<br/>• Calibrated $\tau = 0.65$ improves `composite_risk` F1 from `68.57%` to **`76.92%`**; median MTTR = **`66.5s`** |
| `curated/benchmarks/milestone6_dashboard_benchmark.json` | JSON | 11-table validation & 4-view dashboard latency | • All 11 curated tables verified (`all_11_tables_valid = True`)<br/>• View 1 (`0.1193s`), View 2 (`0.7136s`), View 3 (`0.1815s`), View 4 (`0.0040s`) — all $< 2.0\text{s}$ |
| `curated/benchmarks/milestone7_scaling_benchmarks.json` | JSON | Strong/Weak scaling, partition sweep & input-size scaling | • Strong scaling `1.69x` speedup (`local[4]`); optimal shuffle partitions = **`16` (`10.00s`)**<br/>• Full 46-day `8.43M`-row run = **`38.09s` (`221,295 rows/s`)**; DF vs. RDD = **`2.86x` speedup** |
| `curated/benchmarks/milestone7_load_recovery.json` | JSON | Multi-speed streaming load, window latency & restart recovery | • `1x..100x` load capacity verified (`2,266–8,989 rows/s`); 5m window latency **`0.35s–9.45s < 60s`**<br/>• Outage backlog (`123,537` events + `19,379` dups) drained to **`0` in `13.7s–18.7s`** with **`0` duplicate alerts** |
| `curated/benchmarks/milestone8_case_studies_demo.json` | JSON | 4 real Wikipedia case studies & backup demo readiness | • `CONFIRMED_NEWS` (*Navi Pillay*), `BREAKING_NEWS_CANDIDATE` (*Katie Zacharia*), `EDIT_WAR_CONTROVERSY` (*Hurricane Isaias (2026)*), `MAJOR_REWRITE` (*Mike Ditka*) |

---

## 4. End-to-End Code Review & Quality Assurance Audit (Milestones 1–8)

| Review Dimension | Files Audited | Review Findings & Engineering Resolutions | Verification Status |
| :--- | :--- | :--- | :---: |
| **1. Schema & Contract Integrity** | `src/models/schemas.py`, `src/verify_milestone1.py`, `src/dashboard/app.py` | • Verified all 8 canonical `StructType` schemas against all 11 materialized Parquet tables (`all_11_tables_valid = True`).<br/>• Resolved field name alignment in `backup_demo.py` (`delta_bytes` & `diff_snippet` in `REVISION_DIFF_SCHEMA`; `country_or_region` in `MAP_POINT_SCHEMA`; computed `rewriting_bytes = gross_bytes - abs(net_bytes)` in `ALERT_SCHEMA`). | **Passed (`8/8` schemas, `11/11` tables)** |
| **2. Data Hygiene & Fault Tolerance** | `src/historical/ingest_curate.py`, `src/config/settings.py` | • Multi-member gzip recovery (`iter_recoverable_gz_lines`) salvaged `269,017` rows from `rc_2026-10-27.jsonl.gz` without data loss.<br/>• `safe_write_parquet()` and `safe_read_parquet(pathGlobFilter="*.parquet")` isolate Spark writes/reads from macOS Finder `Icon\r` metadata files. | **Passed (`100.0%` valid event retention)** |
| **3. Anomaly Scoring & Explainability** | `src/anomaly/window_features.py`, `src/anomaly/scoring_sparkml.py`, `src/anomaly/evaluate_alerts.py` | • Session-clustered window editor scaling (`1/18` for `5m`, `1/6` for `1h`) prevents false triggers on hyperactive pages while preserving quiet-page sensitivity.<br/>• Exact additive parity (`sum(feature_contributions.values()) == historical_score`) and $\ge 2$ human-readable `reasons` (`avg = 4.03`) verified on all `3,564` alerts. | **Passed (`5/5` synthetic recall, `99.7%` ML precision)** |
| **4. Contextual Enrichment & Privacy** | `src/enrichment/graph_and_diffs.py`, `src/enrichment/geospatial_map.py`, `src/enrichment/revert_risk_monitor.py` | • `action=compare` HTML parser + revision metadata fallback achieves `100.0%` usable diff coverage (`>= 90%` target).<br/>• 2-tier Wikipedia/Wikidata subject resolver maps `75.62%` of alerts with `0` editor IP addresses and preserves `24.38%` unlocated conceptual alerts.<br/>• Lift Wing ML + heuristic fallback achieves `100.0%` score coverage (`>= 95%` target) and empirical calibration at $\tau = 0.65$ lifts F1 to `76.92%`. | **Passed (All 4 enrichment SLAs met)** |
| **5. Scaling, Streaming Load & Checkpoint Recovery** | `src/benchmarks/scaling_experiments.py`, `src/benchmarks/load_and_recovery_benchmark.py` | • Flat top-level `*.parquet` wave staging in `run_load_and_window_latency_benchmark()` ensures Spark Structured Streaming file source processes every microbatch wave (`2,266.2 rows/s`, closed 5m window latency `9.454s < 60.0s`).<br/>• Controlled stop/restart with `123,537` backlog events + `19,379` duplicates drained to `0` in `18.652s` with `0` duplicate alerts (`39/39` reference parity). | **Passed (All Milestone 7 benchmarks verified)** |
| **6. Backup Demo & UI Serving Latency** | `src/demo/backup_demo.py`, `dashboards/data_service.py`, `dashboards/pages/system.py`, `run-demo.sh` | • Enforced distinct article selection (`used_titles: set[str]`) across all 4 case studies (`Navi Pillay`, `Katie Zacharia`, `Hurricane Isaias (2026)`, `Mike Ditka`).<br/>• Mapped `ingestion_summary.json`, `milestone7_scaling_benchmarks.json`, and `milestone7_load_recovery.json` into `/system` health cards and charts.<br/>• All 4 dashboard views query in `< 0.61s` (`< 2.0s` SLA) and all 5 HTTP routes return `200 OK`. | **Passed (`45/45` unit tests, `< 0.61s` max latency)** |

---

## 5. Quick Reference: Commands to Run All 8 Milestones

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

# 8. Milestone 6.1: Build Link Graph Spillover & Revision Diffs
.venv/bin/python3 Repo/src/enrichment/graph_and_diffs.py

# 9. Milestone 6.2: Build Article-Subject Geospatial Map & Update Alert Locations
.venv/bin/python3 Repo/src/enrichment/geospatial_map.py

# 10. Milestone 6.3: Run Lift Wing Revert Risk Scoring & 48h Revert Threshold Evaluation
.venv/bin/python3 Repo/src/enrichment/revert_risk_monitor.py

# 11. Milestone 6.4: Benchmark All 11 Curated Tables & 4 Dashboard Views (< 2.0s)
.venv/bin/python3 Repo/src/dashboard/app.py --benchmark-only

# 12. Milestone 7.1: Run Strong/Weak Scaling, Shuffle Partition Sweep & Input-Size Benchmarks
.venv/bin/python3 Repo/src/benchmarks/scaling_experiments.py

# 13. Milestone 7.2: Run Multi-Speed Streaming Load, Window Latency & Stop/Restart Recovery
.venv/bin/python3 Repo/src/benchmarks/load_and_recovery_benchmark.py

# 14. Milestone 8.1: Run Pre-Flight Verification & 4 Real Case-Study Walkthrough
.venv/bin/python3 Repo/src/demo/backup_demo.py --verify-only

# 15. Launch the Pre-Warmed Interactive WikiPulse Backup Demo App (http://127.0.0.1:8080)
.venv/bin/python3 Repo/src/demo/backup_demo.py --port 8080

# 16. Run Full Automated Unit & Integration Test Suite (45 tests across Milestones 1–8)
PYTHONPATH=Repo .venv/bin/python3 -m unittest discover -s Repo/tests -v
```

