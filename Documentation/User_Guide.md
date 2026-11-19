# WikiPulse — User Guide

**System:** WikiPulse — Real-Time Detection and Analysis of Wikipedia Knowledge Shifts  
**Authors:** Kshama Jain, Senthil Raj K, Pothukanuri Sai Venkat, Akash H S (Team QuadCore)  
**Institution:** Indian Institute of Science (IISc) — DA231o: Data Engineering at Scale  

---

## Table of Contents

1. [Historical Data](#1-historical-data)
   - [1.1 How to Collect Historical Data (Or Download Pre-Collected Archives)](#11-how-to-collect-historical-data-or-download-pre-collected-archives)
   - [1.2 End-to-End Lifecycle: What Happens After Collecting Data](#12-end-to-end-lifecycle-what-happens-after-collecting-data)
   - [1.3 Step-by-Step Pipeline Execution Guide & Exact Commands](#13-step-by-step-pipeline-execution-guide--exact-commands)
   - [1.4 Master Command Reference: Build All Required Historical Data](#14-master-command-reference-build-all-required-historical-data)
2. [Live Streaming Using Apache Kafka & Spark Structured Streaming](#2-live-streaming-using-apache-kafka--spark-structured-streaming)
   - [2.1 System Prerequisites & Environment Setup](#21-system-prerequisites--environment-setup)
   - [2.2 One-Command Supervised Live Stack (`./run-local.sh`)](#22-one-command-supervised-live-stack-run-localsh)
   - [2.3 What Happens Under the Hood During Live Streaming](#23-what-happens-under-the-hood-during-live-streaming)
   - [2.4 Manual Service-by-Service Execution & Kafka Verification Commands](#24-manual-service-by-service-execution--kafka-verification-commands)
   - [2.5 Switching Between Live Stream and Upstream Historical Catch-Up (`?since=`)](#25-switching-between-live-stream-and-upstream-historical-catch-up-since)
   - [2.6 Historical Kafka Replay at Configurable Speeds (`1x` to `100x`)](#26-historical-kafka-replay-at-configurable-speeds-1x-to-100x)
   - [2.7 Docker-Free Local File-Broker Fallback Mode](#27-docker-free-local-file-broker-fallback-mode)
3. [Working on Dashboards with Historical & Curated Data](#3-working-on-dashboards-with-historical--curated-data)
   - [3.1 Launching the Dashboards Over the Curated Historical Lakehouse](#31-launching-the-dashboards-over-the-curated-historical-lakehouse)
   - [3.2 Global Navigation, Workspace Controls & Activity Console](#32-global-navigation-workspace-controls--activity-console)
   - [3.3 Screen-by-Screen Guide to Analyzing Historical Data (`01` to `08`)](#33-screen-by-screen-guide-to-analyzing-historical-data-01-to-08)
   - [3.4 Guided Case-Study Investigation Workflow](#34-guided-case-study-investigation-workflow)

---

## 1. Historical Data

The Historical Data subsystem transforms raw multi-week English Wikipedia change logs and Wikimedia Analytics time series into a partitioned **11-table Curated Parquet Lakehouse** (`data/curated/`) and **12 JSON evaluation & benchmark artifacts** (`data/curated/benchmarks/`).

### 1.1 How to Collect Historical Data (Or Download Pre-Collected Archives)

You can populate the raw historical archives (`rc_YYYY-MM-DD.jsonl.gz` and `revisions_YYYY-MM-DD.jsonl.gz`) in two ways:
- **Option A:** Collect directly from the MediaWiki Action API using the built-in resumable collectors ([`enwiki_collect_recentchanges.py`](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/enwiki_collect_recentchanges.py) and [`enwiki_collect_revisions.py`](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/enwiki_collect_revisions.py)).
- **Option B:** Download the pre-collected 55-day English Wikipedia archive (`2026-09-15` to `2026-11-08`, `17,441,561` raw events, `1.18 GB` compressed) from the shared Google Drive folder.

#### Option A: Direct Collection via Resumable MediaWiki API Collectors

1. **Collect RecentChanges (`enwiki_collect_recentchanges.py`):**
   - Queries `https://en.wikipedia.org/w/api.php?action=query&list=recentchanges` (`rclimit=500`, `maxlag=5`, polite sequential backoff).
   - Captures `rcid`, `pageid`, `revid`, `old_revid`, `oldlen`, `newlen`, `bot`, `minor`, `new`, `user`, `userid`, `comment`, `timestamp`, `tags`, `loginfo`, and `sha1`.
   - Automatically splits incoming events by UTC date into `data/enwiki_recentchanges/rc_YYYY-MM-DD.jsonl.gz` and saves atomic resume tokens after every batch to `data/enwiki_recentchanges/checkpoints/<start>__<end>.json`. If interrupted (`Ctrl+C` or network drop), re-running the exact same command resumes from the last committed token without losing progress.

   ```bash
   # Run from Repo/ directory:
   cd Repo

   # 1. Smoke test (3 API requests only)
   .venv/bin/python3 enwiki_collect_recentchanges.py --test

   # 2. Collect a specific UTC date or multi-day window [--start, --end)
   .venv/bin/python3 enwiki_collect_recentchanges.py \
       --start 2026-09-15T00:00:00Z \
       --end 2026-09-16T00:00:00Z \
       --max-gb 8.0

   # 3. Collect a multi-week range (resumable across runs, capped at 8.0 GB on disk)
   .venv/bin/python3 enwiki_collect_recentchanges.py \
       --start 2026-09-15T00:00:00Z \
       --end 2026-11-09T00:00:00Z \
       --max-gb 8.0
   ```

2. **Collect Permanent Revision Histories (`enwiki_collect_revisions.py`):**
   - Queries `https://en.wikipedia.org/w/api.php?action=query&list=allrevisions` for permanent revision metadata (`revid`, `parentid`, `size`, `tags`, `user`, `comment`) and writes to `data/enwiki_revisions/revisions_YYYY-MM-DD.jsonl.gz`.

   ```bash
   # Collect a 6-hour or full-day revision window
   .venv/bin/python3 enwiki_collect_revisions.py \
       --start 2026-09-15T00:00:00Z \
       --end 2026-09-16T00:00:00Z \
       --max-gb 8.0
   ```

#### Option B: Download the Pre-Collected 55-Day Raw Dataset & Place in Repository

If you prefer not to wait for multi-week API collection, download the pre-collected raw dataset from Google Drive:
- **Shared Google Drive Folder:** `https://drive.google.com/drive/folders/1giibBLj6IKZI-bdIX65DtQvdzAEwYXCt?usp=drive_link`

[`Repo/src/config/settings.py`](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/config/settings.py) resolves the active data directory (`HISTORICAL_DATA_DIR`) in the following priority order:
1. `WIKIPULSE_DATA_DIR` environment variable (if explicitly set).
2. Sibling `../historical/data/` directory (`Project-WikiPulse/historical/data/`) if present.
3. Repository-local `Repo/data/` directory (default for a standalone `Repo/` checkout).

Place the downloaded `rc_*.jsonl.gz` files into `data/enwiki_recentchanges/`:

```bash
cd Repo
mkdir -p data/enwiki_recentchanges data/enwiki_revisions data/analytics data/curated
mv ~/Downloads/enwiki_recentchanges/* data/enwiki_recentchanges/
```

> [!NOTE]
> You do not need to manually download `data/analytics/`—running the baseline builder (`src/historical/build_baselines.py`) automatically creates and populates `data/analytics/` (including `pageviews_top_daily.json`, `edits_daily_*.json`, `editors_daily.json`, and `pages/views_*.json`).

---

### 1.2 End-to-End Lifecycle: What Happens After Collecting Data

Once raw `rc_YYYY-MM-DD.jsonl.gz` files are collected on disk, WikiPulse executes a 9-stage distributed PySpark processing and evaluation pipeline. Every stage reads from and writes to the shared Curated Parquet Lakehouse (`data/curated/`).

```text
┌──────────────────────────────────────────────────────────────────────────────┐
│                 PHASE A: RAW CURATION & HISTORICAL BASELINES                 │
├──────────────────────────────────────────────────────────────────────────────┤
│  Raw Archives: data/enwiki_recentchanges/rc_*.jsonl.gz (55 Days, 17.44M)    │
│       │                                                                      │
│       ▼                                                                      │
│  [Stage 1] Raw Cleanup & Parquet Ingestion (ingest_curate.py)                │
│            -> curated/rc_events (10.19M deduplicated rows)                   │
│       │                                                                      │
│       ▼                                                                      │
│  [Stage 2] Per-Page Zero-Inclusive Baselines (build_baselines.py)            │
│            -> curated/page_baselines (3.31M pages) + data/analytics/         │
│       │                                                                      │
│       ▼                                                                      │
│  [Stage 3] Churn, Bot Ratio & Surge Rankings (rankings_benchmark.py)         │
│            -> curated/churn_bot_summary & curated/rankings_daily             │
└───────────────────────────────────┬──────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│           PHASE B: WINDOW EXTRACTION, MULTI-DETECTOR & SPARK ML              │
├──────────────────────────────────────────────────────────────────────────────┤
│  [Stage 4] 5m & 1h Window Feature Extraction (window_features.py)            │
│            -> curated/page_windows_5m (113k) & page_windows_1h (60k)         │
│       │                                                                      │
│       ▼                                                                      │
│  [Stage 5] Multi-Detector & Spark ML Scoring (scoring_sparkml.py)            │
│            -> curated/alerts (Fixed + Historical Score + KMeans/RF)          │
│       │                                                                      │
│       ▼                                                                      │
│  [Stage 6] Synthetic Injection, False-Alert & Replay Audit                   │
│            (evaluate_alerts.py)                                              │
└───────────────────────────────────┬──────────────────────────────────────────┘
                                    │
          ┌─────────────────────────┼─────────────────────────┐
          ▼                         ▼                         ▼
┌───────────────────┐     ┌───────────────────┐     ┌──────────────────────────┐
│    [Stage 7A]     │     │    [Stage 7B]     │     │        [Stage 7C]        │
│  Link Graph &     │     │  2-Tier Subject   │     │  Lift Wing Revert Risk   │
│  Compare Diffs    │     │  Geospatial Map   │     │  & 48h Calibration       │
│  (graph_and_diffs)│     │  (geospatial_map) │     │  (revert_risk_monitor)   │
│  -> graph_edges   │     │  -> map_points    │     │  -> revert_risk_eval     │
│  -> revision_diffs│     │                   │     │                          │
└─────────┬─────────┘     └─────────┬─────────┘     └────────────┬─────────────┘
          │                         │                            │
          └─────────────────────────┼────────────────────────────┘
                                    ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                 PHASE D: BENCHMARKS & PRE-WARMED DASH SERVING                │
├──────────────────────────────────────────────────────────────────────────────┤
│  [Stage 8] Scaling, Shuffle & Streaming Recovery Benchmarks                  │
│            (scaling_experiments.py & load_and_recovery_benchmark.py)         │
│       │                                                                      │
│       ▼                                                                      │
│  [Stage 9] 11-Table Schema Audit & Pre-Warmed Dash UI Server                 │
│            (dashboard/app.py & demo/backup_demo.py -> http://127.0.0.1:8080) │
└──────────────────────────────────────────────────────────────────────────────┘
```

#### Detailed Breakdown of Each Post-Collection Stage

| Stage | Script | Input Data | What Happens Under the Hood | Materialized Outputs |
| :---: | :--- | :--- | :--- | :--- |
| **1** | [`src/historical/ingest_curate.py`](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/historical/ingest_curate.py) | `enwiki_recentchanges/rc_*.jsonl.gz` | 1. **Gzip Stream Recovery:** Scans raw byte headers (`\x1f\x8b\x08`) via `zlib.decompressobj` to recover interrupted multi-member gzip files without data loss.<br/>2. **Noise Filtering & Dedup:** Drops `type == 'categorize'` maintenance noise (~41.6% of raw feed) and deduplicates globally by `rcid`.<br/>3. **Normalization:** Casts all records to the 29-column `RAW_EDIT_SCHEMA` (extracting `delta`, `abs_delta`, `is_anon` for `~2026-...` temporary accounts, `is_reverted`, and `is_revert_action`).<br/>4. **Partitioned Write:** Stages in `/tmp` and atomically writes Parquet partitioned by `day=YYYY-MM-DD`. | • `curated/rc_events/` (`10,190,119` rows)<br/>• `curated/benchmarks/ingestion_summary.json` |
| **2** | [`src/historical/build_baselines.py`](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/historical/build_baselines.py) | `curated/rc_events/`<br/>+ Wikimedia Analytics REST API | 1. **Date Split:** Filters to the 47-day baseline period (`2026-09-15` .. `2026-10-31`, $D=47\text{ days}$, $H=1,128\text{ hours}$).<br/>2. **Zero-Inclusive Moments:** Computes exact calendar daily and hourly means and standard deviations ($\mu = \sum x / H$, $\sigma = \sqrt{\sum x^2 / H - \mu^2}$) across all $1,128$ hours (including zero-edit hours) with variance floors.<br/>3. **Activity Stratification:** Assigns each of the `3,307,257` pages to an `activity_tier` (`quiet`, `moderate`, `active`, `hyperactive`).<br/>4. **Analytics Enrichment:** Automatically populates `data/analytics/` and joins per-article daily pageviews, recording `last_available_date` and `has_missing_data`. | • `curated/page_baselines/` (`3,307,257` rows)<br/>• `data/analytics/*.json` |
| **3** | [`src/historical/rankings_benchmark.py`](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/historical/rankings_benchmark.py) | `curated/rc_events/`,<br/>`curated/page_baselines/` | 1. **Churn & Rewriting Summary:** Aggregates daily $\times$ namespace Human, Anonymous/Temp, and Bot edits, decomposing byte movement into Gross Churn ($\sum |\Delta|$), Net Growth ($\sum \Delta$), and Rewriting Volume ($\sum |\Delta| - |\sum \Delta|$).<br/>2. **Surge vs. Raw Rankings:** Joins daily article activity with `page_baselines` to rank articles by both raw edits (`raw_edit_rank`) and baseline-normalized velocity surge (`baseline_surge_rank`).<br/>3. **Verification Benchmarks:** Runs the 2,000-event pure-Python vs. Spark manual audit, the 7-day PySpark RDD vs. DataFrame benchmark, and the raw `JSONL.gz` vs. stored Parquet query latency benchmark. | • `curated/churn_bot_summary/` (`1,335` rows)<br/>• `curated/rankings_daily/` (`20,333` rows)<br/>• `curated/benchmarks/*.json` (Historical & RDD/DF report) |
| **4** | [`src/anomaly/window_features.py`](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/anomaly/window_features.py) | `curated/rc_events/`,<br/>`curated/page_baselines/` | 1. **Tumbling Window Aggregation:** Aggregates post-baseline evaluation events (`2026-11-01` .. `2026-11-08`, or all 55 days with `--all-days`) into **5-minute (`5m`)** and **1-hour (`1h`)** page windows.<br/>2. **Session-Clustered Baseline Normalization:** Joins with `page_baselines` using calibrated session-clustered divisors ($1/18$ of daily editors for `5m`, $1/6$ for `1h`) to compute `velocity_zscore`, `editor_surge_ratio`, `churn_zscore`, and `consensus_factor`. | • `curated/page_windows_5m/` (`113,754` rows)<br/>• `curated/page_windows_1h/` (`60,714` rows)<br/>• `curated/benchmarks/window_features_summary.json` |
| **5** | [`src/anomaly/scoring_sparkml.py`](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/anomaly/scoring_sparkml.py) | `curated/page_windows_5m/`,<br/>`curated/page_windows_1h/`,<br/>`curated/page_baselines/` | 1. **Detector A (Fixed Rule):** Flags mainspace windows with $\ge 6$ human edits & $\ge 3$ editors in `5m` (or $\ge 15$ edits & $\ge 4$ editors in `1h`).<br/>2. **Detector B (Explainable Historical Score `0–100`):** Combines velocity (`35 pts`), editor diversity (`25 pts`), byte churn (`20 pts`), consensus/dispute (`10 pts`), and pageview corroboration (`10 pts`) with low-surge/bot dampening so $\sum \text{feature\_contributions} \equiv \text{historical\_score}$. Classifies into `CONFIRMED_NEWS`, `BREAKING_NEWS_CANDIDATE`, `EDIT_WAR_CONTROVERSY`, or `MAJOR_REWRITE` and attaches $\ge 2$ human-readable `reasons`.<br/>3. **Detector C (Spark MLlib Pipeline):** Standardizes 8 behavioral features (`VectorAssembler` $\rightarrow$ `StandardScaler`), fits unsupervised `KMeans(k=6)` to compute Euclidean distance from the normal baseline centroid (`f_centroid_distance`), and trains a 40-tree `RandomForestClassifier` to produce `spark_ml_score`.<br/>4. **Deterministic Primary Key:** Computes SHA-256 `alert_id` (`alt_<16_hex>`). | • `curated/alerts/` (`1,321` eval / `3,564` full-lake alerts)<br/>• `curated/benchmarks/*.json` (Scoring & Spark ML metrics) |
| **6** | [`src/anomaly/evaluate_alerts.py`](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/anomaly/evaluate_alerts.py) | `curated/alerts/`,<br/>`curated/rc_events/`,<br/>`curated/page_baselines/` | 1. **Synthetic Pattern Injection:** Injects 5 positive knowledge-shift patterns + 2 negative controls and verifies 100% recall (`5/5`), exact additive `feature_contributions`, and `0` false triggers.<br/>2. **False-Alert & Precision-Recall Audit:** Compares Detectors A, B, and C across the evaluation dataset, a reviewed 200-window sample, and thresholds `50..95`.<br/>3. **Replay Idempotency & Hash Parity:** Simulates a replay restart with 30% duplicate events and verifies `0` duplicate alerts and 100% Python $\leftrightarrow$ Spark SHA-256 `alert_id` parity. | • `curated/benchmarks/*.json` (Synthetic & false-alert evaluation report) |
| **7A** | [`src/enrichment/graph_and_diffs.py`](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/enrichment/graph_and_diffs.py) | `curated/alerts/`,<br/>`curated/page_windows_*`,<br/>`curated/rc_events/` | 1. **Topic Spillover Graph:** Fetches outgoing links (`prop=links`) and incoming backlinks (`list=backlinks`) for top alerted articles (cached in `curated/cache/graph/`) and joins against `page_windows` to flag `co_edited_in_window` neighbors.<br/>2. **Sanitized Revision Diffs:** Fetches `action=compare` diffs for top alerted revisions (cached in `curated/cache/diffs/`), strips all HTML into plain-text snippets, counts added/removed lines and external URLs, and falls back to revision metadata if a revision was deleted. | • `curated/graph_edges/` (`606` rows)<br/>• `curated/revision_diffs/` (`100` rows)<br/>• `curated/benchmarks/*.json` (Graph & diff coverage report) |
| **7B** | [`src/enrichment/geospatial_map.py`](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/enrichment/geospatial_map.py) | `curated/alerts/` | **2-Tier Privacy-Preserving Subject Resolver:** Resolves article-subject coordinates (never editor IPs) via Tier-1 Wikipedia `prop=coordinates|pageprops` and Tier-2 3-hop Wikidata `wbgetentities` (`P625` direct $\rightarrow$ `P276`/`P131`/`P159` related entity $\rightarrow$ `P17`/`P495`/`P27` country centroid), tagging `location_source` and `location_quality` while keeping unlocated conceptual alerts valid. Updates `has_location` on `curated/alerts`. | • `curated/map_points/` (`158`–`160` rows)<br/>• Updates `curated/alerts/`<br/>• `curated/benchmarks/*.json` (Geospatial coverage report) |
| **7C** | [`src/enrichment/revert_risk_monitor.py`](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/enrichment/revert_risk_monitor.py) | `curated/rc_events/`,<br/>`curated/alerts/` | **Lift Wing Revert Risk & 48h Calibration:** Pre-filters routine edits in Spark (~75% avoided), scores candidate revisions via Wikimedia Lift Wing (`revertrisk-language-agnostic:predict`, cached in `curated/cache/liftwing/`) with heuristic fallback, matches scored revisions to actual 48-hour revert outcomes (`time_to_revert_sec`, `reverter_is_bot`), and evaluates threshold curves across $\tau \in [0.20, 0.90]$. | • `curated/revert_risk_eval/` (`120` rows)<br/>• `curated/benchmarks/*.json` (Revert risk & calibration report) |
| **8** | [`src/benchmarks/scaling_experiments.py`](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/benchmarks/scaling_experiments.py)<br/>& [`load_and_recovery_benchmark.py`](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/benchmarks/load_and_recovery_benchmark.py) | `curated/rc_events/`,<br/>`curated/page_baselines/` | 1. **Scaling Suite:** Runs Strong Scaling (`local[1..6]`), Weak Scaling (`4d/core`), Shuffle Partition Sweep (`4..128`), Input-Size Scaling (`1d..46d`), and RDD vs. DataFrame telemetry (recording runtime, shuffle MB, peak RSS memory MB, and storage MB).<br/>2. **Streaming Load & Recovery:** Evaluates `1x..100x` streaming load capacity, closed 5m window latency (`< 60s`), and controlled stop/restart outage backlog recovery (`123,537` events $\rightarrow$ `0` backlog with `0` duplicate alerts). | • `curated/benchmarks/*.json` (Scaling & load recovery telemetry) |
| **9** | [`src/dashboard/app.py`](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/dashboard/app.py)<br/>& [`src/demo/backup_demo.py`](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/demo/backup_demo.py) | All 11 `curated/*` tables & `benchmarks/*.json` | Audits schema validity across all 11 Parquet tables, verifies `< 2.0s` query latency across all dashboard views, extracts 4 real Wikipedia case studies across all 4 alert classes, and pre-warms the interactive Dash + Plotly server. | • `curated/benchmarks/*.json` (Dashboard latency & case studies report) |

---

### 1.3 Step-by-Step Pipeline Execution Guide & Exact Commands

All commands below assume your terminal is in the repository root (`Repo/`) and the virtual environment (`.venv`) is active.
*(If running from the parent `Project-WikiPulse/` workspace root, prefix script paths with `Repo/` and set `PYTHONPATH=Repo`.)*

#### Step 1: Clean Raw Overlap, Recover Gzip Streams & Ingest Curated Parquet (`curated/rc_events`)
Convert all `rc_YYYY-MM-DD.jsonl.gz` files into the partitioned `curated/rc_events/day=YYYY-MM-DD/` Parquet lakehouse:

```bash
cd Repo

# Standard ingestion (reads all rc_*.jsonl.gz, drops categorize noise, deduplicates by rcid, writes Parquet)
PYTHONPATH=. .venv/bin/python3 src/historical/ingest_curate.py

# Optional: Also repair multi-member gzip files and deduplicate raw .jsonl.gz files in-place before Spark ingestion
PYTHONPATH=. .venv/bin/python3 src/historical/ingest_curate.py --clean-raw-overlap --days all
```

#### Step 2: Build Zero-Inclusive Per-Page Historical Baselines (`curated/page_baselines`)
Compute per-page daily and hourly baselines ($\mu, \sigma$), activity tiers (`quiet`, `moderate`, `active`, `hyperactive`), and Wikimedia Analytics daily pageview joins over the baseline window (`2026-09-15` to `2026-10-31`):

```bash
# Default 47-day baseline window (2026-09-15 .. 2026-10-31) + top 25 active article pageview fetch
PYTHONPATH=. .venv/bin/python3 src/historical/build_baselines.py

# Optional: Custom baseline date range or offline-only pageview join (--fetch-top-pageviews 0)
PYTHONPATH=. .venv/bin/python3 src/historical/build_baselines.py \
    --start 2026-09-15 \
    --end 2026-10-31 \
    --fetch-top-pageviews 30
```

#### Step 3: Build Knowledge Churn, Bot Summaries, Daily Rankings & Run Storage/RDD Benchmarks (`curated/churn_bot_summary` & `curated/rankings_daily`)
Generate the daily $\times$ namespace churn/rewriting rollups and daily baseline-surge rankings, and execute the 2,000-event manual sample audit, 7-day RDD vs. DataFrame benchmark, and raw `JSONL.gz` vs. Parquet query latency benchmark:

```bash
PYTHONPATH=. .venv/bin/python3 src/historical/rankings_benchmark.py
```

#### Step 4: Extract 5-Minute & 1-Hour Page Window Features (`curated/page_windows_5m` & `curated/page_windows_1h`)
Aggregate raw events into 5-minute (`5m`) and 1-hour (`1h`) tumbling windows and join with `curated/page_baselines` to compute `velocity_zscore`, `editor_surge_ratio`, `churn_zscore`, `rewriting_bytes`, and `consensus_factor`:

```bash
# Default: Build 5m and 1h windows for the post-baseline evaluation period (2026-11-01 .. 2026-11-08)
PYTHONPATH=. .venv/bin/python3 src/anomaly/window_features.py

# Optional: Build 5m and 1h windows across ALL 55 days in the curated lake
PYTHONPATH=. .venv/bin/python3 src/anomaly/window_features.py --all-days
```

#### Step 5: Run Multi-Detector Anomaly Scoring & Spark ML Pipeline (`curated/alerts`)
Score all 5-minute and 1-hour windows using **Detector A (Fixed Threshold)**, **Detector B (0–100 Explainable Historical Score)**, and **Detector C (Spark ML `KMeans(k=6)` + `RandomForestClassifier(numTrees=40)`)**, and materialize `curated/alerts`:

```bash
PYTHONPATH=. .venv/bin/python3 src/anomaly/scoring_sparkml.py --top-show 15
```

#### Step 6: Run Synthetic Pattern Injection, False-Alert & Replay Idempotency Evaluation
Execute the controlled synthetic knowledge-shift injection test (`5` positive shift patterns + `2` negative controls), compare false-alert rates and threshold calibration curves across Detectors A, B, and C, and verify replay idempotency (`0` duplicate alerts after 30% duplicate injection):

```bash
PYTHONPATH=. .venv/bin/python3 src/anomaly/evaluate_alerts.py
```

#### Step 7: Build Contextual Enrichment Tables (`graph_edges`, `revision_diffs`, `map_points`, `revert_risk_eval`)
Enrich the detected alerts with 1-hop outgoing links/backlinks and co-edit spillover flags, HTML-sanitized MediaWiki `action=compare` revision diffs, 2-tier Wikipedia/Wikidata subject geospatial coordinates, and Wikimedia Lift Wing Revert Risk ML scores calibrated against 48-hour actual reverts:

```bash
# 7A. Build Related-Page Link/Backlink Spillover Graph & Revision Diffs
# (Add --offline to use cached API responses in curated/cache/ without contacting Wikipedia)
PYTHONPATH=. .venv/bin/python3 src/enrichment/graph_and_diffs.py --max-alert-pages 36 --max-revisions 100

# 7B. Build 2-Tier Wikipedia/Wikidata Article-Subject Geospatial Map & Update alerts.has_location
PYTHONPATH=. .venv/bin/python3 src/enrichment/geospatial_map.py --max-pages 160

# 7C. Run Lift Wing Revert Risk Scoring, 48h Revert Matching & Threshold Calibration
PYTHONPATH=. .venv/bin/python3 src/enrichment/revert_risk_monitor.py --max-candidates 120
```

#### Step 8: Execute Engineering Scale, Shuffle, Streaming Load & Failure-Recovery Benchmarks
Run the distributed systems performance experiments (Strong Scaling `local[1..6]`, Weak Scaling `4d/core`, Shuffle Partition Sweep `4..128`, Input-Size Scaling `1d..46d`, Multi-Speed Streaming Load `1x..100x`, and Controlled Stop/Restart Backlog Drain):

```bash
# 8A. Strong/Weak Scaling, Shuffle Partition Sweep, Input-Size Curve & RDD vs. DataFrame Telemetry
PYTHONPATH=. .venv/bin/python3 src/benchmarks/scaling_experiments.py

# 8B. Multi-Speed (1x, 10x, 50x, 100x) Streaming Load, 5m Window Latency & Stop/Restart Recovery
PYTHONPATH=. .venv/bin/python3 src/benchmarks/load_and_recovery_benchmark.py --day 2026-10-15
```

#### Step 9: Verify All 11 Curated Tables, 12 Benchmarks & Dashboard Query Latencies
Run the end-to-end readiness audit and case-study extraction:

```bash
# Verify all 11 Parquet tables, all < 2.0s view query latencies, and all Dash HTTP routes
PYTHONPATH=. .venv/bin/python3 src/dashboard/app.py --benchmark-only

# Run full pre-flight verification + extract the 4 real Wikipedia case studies
PYTHONPATH=. .venv/bin/python3 src/demo/backup_demo.py --verify-only
```

---

### 1.4 Master Command Reference: Build All Required Historical Data

Copy and run the following complete sequence from the `Repo/` directory to build all **11 Curated Parquet Tables** and all **12 Benchmark & Evaluation JSON Artifacts** from scratch:

```bash
cd Repo

# 1. Ingest & curate raw rc_*.jsonl.gz archives -> curated/rc_events
PYTHONPATH=. .venv/bin/python3 src/historical/ingest_curate.py

# 2. Build zero-inclusive per-page baselines & pageview joins -> curated/page_baselines
PYTHONPATH=. .venv/bin/python3 src/historical/build_baselines.py

# 3. Build churn/bot summaries, daily surge rankings & storage benchmarks
#    -> curated/churn_bot_summary & curated/rankings_daily
PYTHONPATH=. .venv/bin/python3 src/historical/rankings_benchmark.py

# 4. Extract 5-minute and 1-hour window features
#    -> curated/page_windows_5m & curated/page_windows_1h
PYTHONPATH=. .venv/bin/python3 src/anomaly/window_features.py

# 5. Score all windows with Detectors A, B & C (Spark MLlib)
#    -> curated/alerts
PYTHONPATH=. .venv/bin/python3 src/anomaly/scoring_sparkml.py

# 6. Run synthetic pattern injection, false-alert audit & replay idempotency verification
PYTHONPATH=. .venv/bin/python3 src/anomaly/evaluate_alerts.py

# 7. Build contextual enrichment tables (Link Graph, Revision Diffs, Geo Map, Revert Risk)
#    -> curated/graph_edges & curated/revision_diffs
PYTHONPATH=. .venv/bin/python3 src/enrichment/graph_and_diffs.py
#    -> curated/map_points (and updates curated/alerts.has_location)
PYTHONPATH=. .venv/bin/python3 src/enrichment/geospatial_map.py
#    -> curated/revert_risk_eval
PYTHONPATH=. .venv/bin/python3 src/enrichment/revert_risk_monitor.py

# 8. Run distributed scaling, shuffle, streaming load & failure-recovery benchmarks
PYTHONPATH=. .venv/bin/python3 src/benchmarks/scaling_experiments.py
PYTHONPATH=. .venv/bin/python3 src/benchmarks/load_and_recovery_benchmark.py

# 9. Audit all 11 tables, benchmark dashboard views (< 2.0s), and extract the 4 case studies
PYTHONPATH=. .venv/bin/python3 src/dashboard/app.py --benchmark-only
PYTHONPATH=. .venv/bin/python3 src/demo/backup_demo.py --verify-only
```

> [!TIP]
> **Fast Offline Re-Build Using Cached API Responses:**  
> If `data/curated/cache/` is already populated (or downloaded from Google Drive), you can append `--offline` to `graph_and_diffs.py`, `geospatial_map.py`, and `revert_risk_monitor.py`, and `--fetch-top-pageviews 0` to `build_baselines.py` to rebuild all 11 Parquet tables with zero external network calls.

#### Auxiliary Historical Preparation Utility (`src/prepare_history.py`)
For standalone streaming-format historical window and baseline generation (`data/week2/historical/edits`, `windows_5m`, `windows_1h`, `baselines_5m`, `baselines_1h`) used by the local streaming detector when `curated/page_baselines` is not present:

```bash
cd Repo
PYTHONPATH=. .venv/bin/python3 -m src.prepare_history \
    --input data/enwiki_recentchanges \
    --output data/week2/historical \
    --start 2026-09-15 \
    --end 2026-11-01 \
    --trust-complete-files
```


---

## 2. Live Streaming Using Apache Kafka & Spark Structured Streaming

The Live Streaming subsystem captures real-time English Wikipedia edits from the Wikimedia EventStreams Server-Sent Events (SSE) feed, publishes them into an **Apache Kafka 3.9.1 (KRaft)** message broker, processes them through **Spark Structured Streaming** (Bronze ingestion $\rightarrow$ 10-minute watermarked 5-minute and 1-hour window aggregations $\rightarrow$ streaming anomaly detection), and serves live updates to the interactive Dash web application.

### 2.1 System Prerequisites & Environment Setup

Before launching the live streaming stack, ensure the following prerequisites are configured on your machine (macOS or Linux):

1. **Required Runtime Tools:**
   - **Python 3.10–3.14** (Python 3.12 or 3.14 recommended; `.venv` is automatically created by `./run-local.sh` if absent).
   - **OpenJDK 17** (`java -version` available on `PATH` or installed at `/opt/homebrew/opt/openjdk@17`).
   - **Docker & Docker Compose v2** (`docker compose version`) with a running container engine (Docker Desktop, Colima, or Podman).
   - **Available Memory & Port:** Keep ~3–4 GB of free RAM for the Kafka container (`768 MB` limit) and the Spark local streaming JVMs, and keep port `8080` (or a custom `--port`) free.

2. **Configure `.env` in `Repo/`:**
   Create or verify `Repo/.env` with your Wikimedia-compliant `WIKIMEDIA_USER_AGENT` and local Kafka settings:

   ```bash
   cd Repo
   cat << 'EOF' > .env
   WIKIMEDIA_USER_AGENT="WikiPulse/1.0 (Senthil Raj K; mailto:senthilrajk@iisc.ac.in)"
   WIKIPULSE_USER_AGENT="WikiPulse/1.0 (Senthil Raj K; mailto:senthilrajk@iisc.ac.in)"
   KAFKA_BOOTSTRAP_SERVERS=localhost:9092
   KAFKA_TOPIC=wiki.raw.edits
   KAFKA_ALERT_TOPIC=wiki.shift.candidates
   SPARK_MASTER=local[2]
   SPARK_DRIVER_MEMORY=512m
   SPARK_TRIGGER_SECONDS=10
   SPARK_MAX_OFFSETS=50000
   EOF
   ```

3. **Run the Prerequisite Check (`--check`):**
   Validate Java 17, Docker daemon readiness, `.env` configuration, and port availability without starting any services:

   ```bash
   cd Repo
   ./run-local.sh --check
   ```

---

### 2.2 One-Command Supervised Live Stack (`./run-local.sh`)

To start the entire real-time streaming platform with a single command, run:

```bash
cd Repo
./run-local.sh --port 8080
```

#### What `./run-local.sh` Automates
1. **Virtual Environment & Dependencies:** Creates `.venv` if needed and verifies all pinned packages in [`requirements.txt`](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/requirements.txt) (`confluent-kafka==2.16.1`, `pyspark==4.2.0`, `pyarrow==26.0.0`, `dash==4.5.0`, `plotly==7.1.0`, `requests==2.34.2`, `python-dotenv==1.2.4`).
2. **Stale Process & Lock Cleanup:** Acquires an exclusive lock on `data/run/local.lock` and cleanly stops any previous WikiPulse supervisor or orphaned Spark JVMs on the selected port.
3. **Kafka Broker & Topic Provisioning:** Starts the `wikipulse-kafka` container defined in [`docker-compose.yml`](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/docker-compose.yml), waits for the broker healthcheck (`/opt/kafka/bin/kafka-broker-api-versions.sh`), and explicitly creates the two 3-partition topics (`wiki.raw.edits` and `wiki.shift.candidates`).
4. **Dependency-Ordered Service Launch:** Starts and monitors 7 local processes, waiting for each stage's readiness barrier before starting the next:
   - `dashboard` (`dashboards.app` on `http://127.0.0.1:8080/`)
   - `collector` (`src.collector` connected to Wikimedia EventStreams SSE)
   - `bronze` (`src.stream_measurements bronze` committing the first Kafka microbatch to Parquet)
   - `windows-5m` & `windows-1h` (`src.stream_measurements windows --duration 5m|1h`)
   - `detection-5m` & `detection-1h` (`src.detection stream --duration 5m|1h`)
5. **Graceful Shutdown (`Ctrl+C`):** Pressing `Ctrl+C` sends `SIGINT` $\rightarrow$ `SIGTERM` to all owned process groups and stops the Kafka container while preserving all Kafka volumes, Parquet data, and Spark checkpoints on disk.

---

### 2.3 What Happens Under the Hood During Live Streaming

```text
┌─────────────────────────────┐       ┌────────────────────────────────────────┐
│ Wikimedia EventStreams SSE  │──────►│ [1] Collector (src/collector.py)       │
│ /v2/stream/recentchange     │       │     • Filters enwiki, ns=0, type=edit  │
└─────────────────────────────┘       │     • Wraps JSON envelope & cursor     │
                                      └───────────────────┬────────────────────┘
                                                          │
                                                          ▼
                                      ┌────────────────────────────────────────┐
                                      │ [2] Apache Kafka 3.9.1 (KRaft)         │
                                      │     Topic: wiki.raw.edits (3 parts)    │
                                      └───────────────────┬────────────────────┘
                                                          │
                                                          ▼
┌─────────────────────────────┐       ┌────────────────────────────────────────┐
│ Historical Baselines        │       │ [3] Spark Bronze Stream                │
│ curated/page_baselines      │       │     (stream_measurements.py bronze)    │
└──────────────┬──────────────┘       │     -> data/raw/wikipedia_edits        │
               │                      └───────┬────────────────────────┬───────┘
               │                              │ (Live Preview)         │
               │                              │                        ▼
               │                              │       ┌────────────────────────┐
               │                              │       │ [4] Watermarked Windows│
               │                              │       │     (5m & 1h, 10m wm)  │
               │                              │       │     -> live_activity/  │
               │                              │       └───────┬────────┬───────┘
               │                              │               │        │
               │                              │   (Finalized) │        ▼
               │                              │               │  ┌─────────────┐
               └──────────────────────────────┼───────────────┼─►│ [5] Anomaly │
                                              │               │  │ Detectors   │
                                              │               │  │ (5m & 1h)   │
                                              │               │  └──────┬──────┘
                                              │               │         │
                                              ▼               ▼         ▼
                                      ┌────────────────────────────────────────┐
                                      │ [7] Dash + Plotly Web UI (:8080)       │
                                      │     • Live Preview (Immediate)         │
                                      │     • Finalized Windows & Alerts       │
                                      └────────────────────────────────────────┘
                                                                        │
                                                                        ▼
                                      ┌────────────────────────────────────────┐
                                      │ [6] Kafka Topic: wiki.shift.candidates │
                                      └────────────────────────────────────────┘
```

#### Key Streaming Concepts & Directories
- **Provisional Live Preview vs. Watermark-Finalized Windows:**
  - **Live Preview (Immediate):** As soon as `bronze` commits its first microbatch (~10 seconds after startup), the dashboard computes provisional 5-minute and 1-hour windows directly from the latest Bronze Parquet files so you see live Wikipedia edits immediately.
  - **Finalized Windows (Watermark-Delayed):** The `windows-5m` and `windows-1h` streaming queries apply a **10-minute event-time watermark** (`withWatermark("ts", "10 minutes")`). A 5-minute window is finalized and emitted to the streaming detector once incoming Wikipedia event timestamps advance 10 minutes past the window's end time (~10–15 minutes after startup for `5m` windows, or rapidly when catching up from a past `--since` timestamp).
- **Live Streaming Storage Layout (`Repo/`):**
  - `data/raw/wikipedia_edits/`: Partitioned Bronze Parquet (`ingestion_date=YYYY-MM-DD`), preserving raw Kafka offsets, partitions, and `is_valid` audit flags.
  - `data/live_activity/bronze_schema.json`: Explicit JSON schema exported by Bronze so downstream file-streaming queries start without schema inference races.
  - `data/live_activity/windows_5m/` & `windows_1h/`: Watermark-finalized streaming window Parquet tables.
  - `data/checkpoints/`: Spark write-ahead checkpoints (`raw_edits`, `page_activity/windows_5m`, `page_activity/windows_1h`, `detection/5m`, `detection/1h`).
  - `data/run/`: Real-time progress and health telemetry (`local_supervisor.json`, `collector-source.progress.json`, `bronze.progress.json`, `windows-5m.progress.json`, `detection-5m.progress.json`).
  - `logs/local/`: Per-service log files (`collector.log`, `bronze.log`, `windows-5m.log`, `windows-1h.log`, `detection-5m.log`, `detection-1h.log`, `dashboard.log`).

---

### 2.4 Manual Service-by-Service Execution & Kafka Verification Commands

If you want to run, inspect, or debug individual components of the Kafka + Spark streaming pipeline in separate terminals (instead of using `./run-local.sh`), follow these exact steps from `Repo/`:

#### Step 1: Start the Apache Kafka Broker & Create Topics
```bash
cd Repo
docker compose up -d --wait kafka

# Explicitly create the raw edits and alert candidate topics (3 partitions, replication factor 1)
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic wiki.raw.edits --partitions 3 --replication-factor 1

docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic wiki.shift.candidates --partitions 3 --replication-factor 1
```

#### Step 2: Start the Wikimedia EventStreams Collector
```bash
# Terminal 1: Connect to live Wikimedia EventStreams and publish to wiki.raw.edits
PYTHONPATH=. .venv/bin/python3 -m src.collector

# Optional: Start collecting from a specific historical UTC timestamp (up to 7-31 days back)
PYTHONPATH=. .venv/bin/python3 -m src.collector --since 2026-11-08T12:00:00Z
```

#### Step 3: Verify & Inspect Messages in Kafka
While the collector is running, you can verify envelope integrity and inspect Kafka messages using either the Python consumer verifier or the PySpark Kafka inspector:

```bash
# Verify 10 messages from Kafka using confluent-kafka Consumer (checks enwiki, ns=0, key==event_id)
PYTHONPATH=. .venv/bin/python3 -m src.verify_kafka --count 10 --timeout 30

# Inspect Kafka topic schema, sample valid edits, and validation counts using PySpark batch read
PYTHONPATH=src .venv/bin/python3 -m src.inspect_kafka --limit 10
```

#### Step 4: Start Spark Structured Streaming Bronze & Window Jobs
```bash
# Terminal 2: Start Bronze ingestion (Kafka -> data/raw/wikipedia_edits + bronze_schema.json)
PYTHONPATH=. .venv/bin/python3 -m src.stream_measurements bronze

# Terminal 3: Start 5-minute watermarked window aggregator (after bronze_schema.json exists)
PYTHONPATH=. .venv/bin/python3 -m src.stream_measurements windows --duration 5m

# Terminal 4: Start 1-hour watermarked window aggregator
PYTHONPATH=. .venv/bin/python3 -m src.stream_measurements windows --duration 1h
```

#### Step 5: Start Streaming Anomaly Detectors (`5m` & `1h`)
```bash
# Terminal 5: Score finalized 5m windows against historical baselines -> curated/alerts & Kafka
PYTHONPATH=. .venv/bin/python3 -m src.detection stream --duration 5m

# Terminal 6: Score finalized 1h windows
PYTHONPATH=. .venv/bin/python3 -m src.detection stream --duration 1h
```

#### Step 6: Start the Dash + Plotly Web Server
```bash
# Terminal 7: Launch the interactive dashboard at http://127.0.0.1:8080
PYTHONPATH=. .venv/bin/python3 -m dashboards.app --port 8080
```

---

### 2.5 Switching Between Live Stream and Upstream Historical Catch-Up (`?since=`)

Wikimedia EventStreams retains approximately 7 to 31 days of recent events in its upstream Kafka brokers. While `./run-local.sh` is running, you can switch the active collector source directly from the **Live Pulse (`/`)** screen without restarting the stack:

1. On **Live Pulse (`/`)**, expand the **WIKIPEDIA STREAM START** control bar (`Live now` vs. `Since timestamp`).
2. Select **Since timestamp**, pick a UTC date (`SINCE (UTC)`) and time (`HH:MM:SS`), and click **Apply stream source**.
3. The local supervisor automatically restarts `src.collector` with `--since <YYYY-MM-DDTHH:MM:SSZ>`, rapidly streams the historical backlog into Kafka and Spark (displaying `CATCHING UP · Wikipedia history is arriving` in the top health badge), and seamlessly transitions into real-time live streaming once caught up.
4. Because historical catch-up events advance the event-time clock rapidly, 5-minute and 1-hour watermarked windows finalize within seconds during catch-up.

---

### 2.6 Historical Kafka Replay at Configurable Speeds (`1x` to `100x`)

To test streaming anomaly detection, watermark progression, and throughput on any collected historical day without depending on live internet connectivity, WikiPulse provides both a **CLI Kafka Replay Engine (`src.replay`)** and an **Interactive UI Kafka & Spark Replay Mode**:

#### A. Interactive UI Kafka & Spark Replay
1. On **Live Pulse (`/`)** or **Page Investigation (`/investigation`)**, switch **SOURCE** from `Live` to `Stored activity`.
2. Choose any captured date in **CAPTURE DATE** and switch **VIEW** from `Day summary` to `Replay`.
3. Expand **Replay settings**:
   - Set **PROCESSING** to `Saved playback` (fast in-memory/Parquet playback) or `Kafka & Spark demo` (publishes the selected day's events into an isolated Kafka replay pipeline and processes them through Spark).
   - Select **PLAYBACK SPEED** (`1×`, `5×`, `20×`, `24×`, `48×`, `50×`, or `100×`).
4. Click **Start replay** (and use the **SEEK WITHIN SELECTED DAY** slider `00:00..24:00` or **Stop replay** at any time).

#### B. Command-Line Isolated Kafka Replay (`src.replay`)
You can also replay any `.jsonl.gz` archive or Parquet table into an isolated Kafka topic (`wiki.replay.edits`) from the terminal:

```bash
cd Repo

# 1. Dry-run validation (parses, deduplicates, and sorts by event time without contacting Kafka)
PYTHONPATH=. .venv/bin/python3 -m src.replay \
    --input data/enwiki_recentchanges/rc_2026-10-15.jsonl.gz \
    --dry-run --limit 5000

# 2. Replay events into isolated topic 'wiki.replay.edits' at 50x speed (or --speed 0 for unpaced max speed)
PYTHONPATH=. .venv/bin/python3 -m src.replay \
    --input data/enwiki_recentchanges/rc_2026-10-15.jsonl.gz \
    --topic wiki.replay.edits \
    --speed 50 \
    --limit 10000
```

To process the isolated replay topic through dedicated Spark Bronze, Window, and Detection jobs without touching your live checkpoints:

```bash
# 1. Drain replay topic into isolated Bronze Parquet (--available-now processes all queued messages and exits)
KAFKA_TOPIC=wiki.replay.edits \
RAW_PARQUET_PATH=data/replay/raw \
RAW_CHECKPOINT_PATH=data/replay/checkpoints/bronze \
PYTHONPATH=. .venv/bin/python3 -m src.stream_measurements bronze \
    --output data/replay/live --available-now

# 2. Compute watermarked 5m windows from isolated Bronze output
RAW_PARQUET_PATH=data/replay/raw \
PYTHONPATH=. .venv/bin/python3 -m src.stream_measurements windows --duration 5m \
    --output data/replay/live \
    --checkpoint-root data/replay/checkpoints/windows \
    --available-now

# 3. Run streaming detector over isolated 5m windows
KAFKA_ALERT_TOPIC=wiki.replay.alerts \
PYTHONPATH=. .venv/bin/python3 -m src.detection stream --duration 5m \
    --windows data/replay/live/windows_5m \
    --baselines data/curated/page_baselines \
    --output data/replay/curated \
    --checkpoint data/replay/checkpoints/detection \
    --available-now
```

---

### 2.7 Docker-Free Local File-Broker Fallback Mode

If you are working on a machine where Docker is not installed or cannot be started, WikiPulse includes a built-in atomic file-backed broker (`FileStreamProducer` in [`src/kafka_common.py`](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/kafka_common.py)) that emulates Kafka partitions and offsets using atomic JSONL spool files (`data/raw/local_broker_spool/`):

```bash
cd Repo

# Terminal 1: Run the live collector in file-broker mode
WIKIPULSE_BROKER_MODE=file PYTHONPATH=. .venv/bin/python3 -m src.collector

# Terminal 2: Run Spark Bronze streaming from the file-broker spool
WIKIPULSE_BROKER_MODE=file PYTHONPATH=. .venv/bin/python3 -m src.stream_measurements bronze
```

---

## 3. Working on Dashboards with Historical & Curated Data

When working with the 55-day Curated Parquet Lakehouse (`data/curated/`), you can launch the full 8-screen interactive Dash + Plotly analytical suite offline with **zero Docker or internet dependencies**.

### 3.1 Launching the Dashboards Over the Curated Historical Lakehouse

#### Option A: One-Command Pre-Warmed Launcher (`./run-demo.sh` / `backup_demo.py`) — *Recommended*
This launcher verifies all 11 curated Parquet tables and 12 benchmark JSON files, prints the 4 representative real-world Wikipedia knowledge-shift case studies in your terminal, pre-warms the server TTL cache across all 8 views, and starts the web server on `http://127.0.0.1:8080`:

```bash
cd Repo

# 1. Launch the full pre-warmed interactive dashboard on http://127.0.0.1:8080
./run-demo.sh --port 8080

# Or invoke the Python entrypoint directly:
PYTHONPATH=. .venv/bin/python3 src/demo/backup_demo.py --port 8080
```

Useful pre-flight flags before starting a presentation or analysis session:
```bash
# Print only the 4 real Wikipedia case studies (Confirmed News, Candidate, Edit War, Major Rewrite) in terminal:
./run-demo.sh --walkthrough-only

# Run full readiness audit (verifies 11 Parquet tables, 12 benchmark JSONs, <2.0s query latency & all HTTP routes):
./run-demo.sh --verify-only
```

#### Option B: Direct Dash App Server with Custom Data Paths (`dashboards.app`)
If you want to launch the dashboard server with the interactive **Activity Workspace & Processing Console** enabled (allowing day-by-day summary browsing and replay over stored days) or point to custom directories:

```bash
cd Repo
PYTHONPATH=. .venv/bin/python3 -m dashboards.app \
    --port 8080 \
    --live data/live_activity \
    --history data/curated \
    --curated data/curated
```

---

### 3.2 Global Navigation, Workspace Controls & Activity Console

Once you open **`http://127.0.0.1:8080`** in your browser, the application shell provides:

1. **Left Sidebar Navigation (`01` to `08`):**
   - Switches instantaneously between the 8 specialized analytical views without full page reloads.
2. **Top Header Bar:**
   - **Stream Health Pill (`stream-health-label`):** Displays live supervisor status (`LIVE · Stream services running`, `CATCHING UP`, `STREAM STOPPED`, or `STREAM OFFLINE` when running in offline historical mode).
   - **Restart Stream Button:** Triggers a token-authenticated checkpoint-safe stream restart when running under `./run-local.sh`.
   - **Dark Mode Toggle:** Switches the entire UI and Plotly charts between light and dark themes (persisted in browser `localStorage`).
3. **Top Filter & Control Bar (Context-Sensitive by Screen):**
   - **SOURCE (`Live` vs. `Stored activity`):** On Screens `01` and `02`, switch to **`Stored activity`** to browse any of the 55 historical dates (`2026-09-15` to `2026-11-08`) using the **CAPTURE DATE** picker (`‹` / `›` step buttons), toggle between **`Day summary`** and **`Replay`**, and choose **`IST`** or **`UTC`** display timezones.
   - **ACTIVITY WINDOW / HORIZON (`5 min`, `1 hour`, `6 hours`):** Switches window aggregation granularity across all charts, rankings, and alert feeds.
   - **ARTICLE Dropdown:** Search or select any active Wikipedia article to filter timelines or jump directly to `/investigation?title=<Article_Title>`.
   - **HISTORICAL RANGE (`15 days`, `1 month`, `3 months`):** On Screen `03 (/historical)`, dynamically slices the longitudinal churn, bot ratio, and ranking series.
   - **Freeze Display / Pause Refresh:** Pauses automatic background chart refreshes while you inspect a specific visual state.
4. **Collapsible Processing Console (`Processing console` bar at the bottom of Screens `01` & `02`):**
   - Click **Processing console** to inspect real-time query execution telemetry:
     - **Rows fetched**, **Events analyzed**, **Data decoded (Apache Arrow payload bytes)**, **Processing time (seconds)**, and **Cache status** (`cached snapshot`, `analyzed from memory`, or `file query`).
   - Click **Download history** (`/api/activity/console/log`) to export the complete NDJSON processing audit log (`wikipulse-processing-history.jsonl`).

---

### 3.3 Screen-by-Screen Guide to Analyzing Historical Data (`01` to `08`)

| Nav # | Screen Name & URL Route | Underlying Curated Parquet Tables | What You Can Analyze & How to Use It |
| :---: | :--- | :--- | :--- |
| **`01`** | **Live Pulse**<br/>`http://127.0.0.1:8080/` | `curated/alerts`,<br/>`curated/page_windows_5m`,<br/>`curated/page_windows_1h`,<br/>`curated/rc_events` | • **Headline KPI Cards:** View total edits, human vs. bot split, peak distinct human editors, net byte change, and gross byte churn in the selected window.<br/>• **Activity & Byte Churn Timelines:** Dual interactive charts showing human vs. bot edit velocity and net vs. gross byte movement over time.<br/>• **Top Active Articles Bar Chart & Activity Ledger:** Click any bar or table row to jump straight to that article's deep-dive investigation.<br/>• **Ranked Knowledge-Shift Alert Feed:** Displays top alerts with badges (`CONFIRMED_NEWS`, `BREAKING_NEWS_CANDIDATE`, `EDIT_WAR_CONTROVERSY`, `MAJOR_REWRITE`), `historical_score` (`0–100`), `spark_ml_score`, and primary human-readable reasons. |
| **`02`** | **Page Investigation**<br/>`http://127.0.0.1:8080/investigation` | `curated/alerts`,<br/>`curated/page_baselines`,<br/>`curated/revision_diffs`,<br/>`curated/revert_risk_eval` | • **Select Any Alerted Article:** Use the **ARTICLE** picker (or click an article from Screen `01`) to open `/investigation?title=<Article>`.<br/>• **Current vs. Normal Baseline Chart:** Compares the article's window edit velocity, distinct editors, and byte churn directly against its 47-day zero-inclusive baseline ($\mu, \sigma$, and `activity_tier`).<br/>• **Additive Score Decomposition (`35/25/20/10/10`):** Inspect exact points contributed by `edit_velocity`, `editor_diversity`, `byte_churn`, `consensus_or_dispute`, and `pageview_corroboration`, plus all $\ge 2$ human-readable `reasons`.<br/>• **Sanitized Revision Diff Cards:** Read plain-text added (`+`) and removed (`-`) snippets from MediaWiki `action=compare`, line counts, external links added, and editor comments.<br/>• **Per-Revision Lift Wing Revert Risk Table:** Inspect `ml_revert_risk`, `heuristic_risk`, `composite_risk`, and whether each revision was actually reverted within 48 hours (`is_reverted_actual`, `time_to_revert_sec`). |
| **`03`** | **Historical Intelligence**<br/>`http://127.0.0.1:8080/historical` | `curated/churn_bot_summary`,<br/>`curated/rankings_daily`,<br/>`curated/page_baselines` | • **Date Range Slicing (`15d`, `1m`, `3m`):** Analyze the 55-day longitudinal archive (`2026-09-15` to `2026-11-08`).<br/>• **Knowledge Churn & Rewriting Volume:** Compare daily Gross Churn ($\sum |\Delta|$), Net Growth ($\sum \Delta$), and **Rewriting Volume** ($\sum |\Delta| - |\sum \Delta|$) to separate constructive growth from back-and-forth rewriting.<br/>• **Contributor Mix & Bot Ratio:** View daily registered human, anonymous/temporary (`~2026-...`), and automated bot edit shares across namespaces.<br/>• **Surge Rank vs. Raw Rank Table:** Demonstrates why static edit counts fail—comparing `raw_edit_rank` (dominated by routine list pages) against `baseline_surge_rank` (`daily_velocity_zscore`).<br/>• **Data Freshness & Completeness Indicators:** Displays `last_available_date`, `last_pageview_date`, and `has_missing_data` flags so missing external metrics are never confused with zero activity. |
| **`04`** | **Anomaly Scoring**<br/>`http://127.0.0.1:8080/anomaly-scoring` | `curated/alerts`,<br/>`curated/benchmarks/*.json` | • **3-Detector Side-by-Side Comparison:** Compares **Detector A (Fixed Rule)**, **Detector B (Explainable Historical Score $\ge 65$)**, and **Detector C (Spark ML $\ge 68$)**.<br/>• **Detector Overlap & Disagreement Cohorts:** Shows the 7-way Venn overlap (`All-3 Consensus`, `B ∩ C Only` quiet-page shifts missed by Detector A, and `Detector A Only` hyperactive routine noise suppressed by Detectors B and C).<br/>• **Score Correlation & Threshold Curves:** Inspect Pearson correlation between Detector B and Detector C scores and Precision/Recall/F1 curves across thresholds `50..95`. |
| **`05`** | **ML Tests & Reports**<br/>`http://127.0.0.1:8080/ml-reports` | `curated/alerts`,<br/>`curated/benchmarks/*.json` | • **Unsupervised `KMeans(k=6)` Behavioral Archetypes:** Inspect cluster sizes, Silhouette score (`0.4229`), standardized 8D centroid profiles, and behavioral cluster labels (`Normal Quiet Baseline`, `Multi-Editor Collaborative Surge`, `Multi-Editor Edit War`, `Single-Editor Major Rewrite`, `Bot-Assisted Maintenance`, `Solo Revert / Cleanup`).<br/>• **Supervised `RandomForestClassifier` Importances:** View feature importance rankings across the 9D feature vector (8 behavioral features + `f_centroid_distance`).<br/>• **Controlled Synthetic Injection Audit:** View pass/fail verification across the 5 injected positive knowledge-shift patterns (`100% recall`) and 2 negative controls (`0 false triggers`).<br/>• **False-Alert Rate Reduction:** Shows false-alert reduction from **65.04%** (Detector A) to **9.14%** (`1.55%` on reviewed 200-window sample) for Detector B and **0.00%** for Detector C. |
| **`06`** | **Context, Graph & Risk**<br/>`http://127.0.0.1:8080/contextual-features` | `curated/graph_edges`,<br/>`curated/revision_diffs`,<br/>`curated/revert_risk_eval`,<br/>`curated/benchmarks/*.json` | • **Related-Page Topic Spillover Graph:** Interactive network visualization of 606 directed outgoing link (`outlink`) and incoming (`backlink`) edges around alerted articles, highlighting the **214 co-edited spillover edges (`35.31%`)** active in the same window.<br/>• **Lift Wing Revert Risk Calibration Curve:** Compares standalone Lift Wing ML, heuristic risk, and composite risk across $\tau \in [0.20, 0.90]$ against actual 48-hour reverts (`75.79%` peak ML F1; `75.06%` routine edits pre-filtered).<br/>• **Bot vs. Human Time-to-Revert (MTTR):** Inspect mean and median reversion speed (`54.0s` overall median MTTR) split by automated bot reverters vs. human editors. |
| **`07`** | **Geospatial Map**<br/>`http://127.0.0.1:8080/geospatial-map` | `curated/map_points`,<br/>`curated/benchmarks/*.json` | • **Global Article-Subject Map (`Scattergeo`):** Plots located knowledge-shift alerts around the world sized by edit activity and colored by alert class (**123 / 160 = 76.88%** resolved; **0 editor IP addresses used**).<br/>• **Source & Quality Provenance Charts:** Breaks down coordinates by `location_source` (`wikipedia_coord`, `wikidata_p625`, `wikidata_p276_p17`) and `location_quality` (`exact_point`, `related_entity_point`, `country_centroid`).<br/>• **Valid Unlocated Alert Registry:** Lists the **37 (`23.12%`)** conceptual/non-spatial alerts (`location_quality = unlocated`) preserved as valid alerts without fabricated coordinates. |
| **`08`** | **Engineering & Health**<br/>`http://127.0.0.1:8080/system` | `curated/benchmarks/*.json`,<br/>`data/run/*.progress.json` | • **Ingestion & Storage Hygiene:** Shows `17,441,561` raw $\rightarrow$ `10,190,119` valid non-categorize events (**100.0% retention**), Parquet vs. JSONL.gz storage footprint, and `< 1.35s` dashboard view query latencies (~14.5x faster than raw JSONL.gz).<br/>• **Distributed Scaling & Shuffle Charts:** Interactive plots for Strong Scaling (`1.69x` speedup on `local[4]`), Weak Scaling (`4d/core`, `249,732 rows/s` peak), Shuffle Partition Sweep (`16` partitions global optimum at `10.00s`), Input-Size Scaling (`8.43M` rows in `38.09s`), and DataFrame vs. RDD speedup (`2.54x–2.86x`).<br/>• **Streaming Load & Failure Recovery:** Displays `1x..100x` replay processing capacity (`8,989–55,834 rows/s`), closed 5m window latency (`0.35s–9.45s < 60s`), and the controlled stop/restart backlog drain curve (`123,537` backlog events + `19,379` duplicates drained to `0` in `13.7s` with `0` duplicate alerts). |

---

### 3.4 Guided Case-Study Investigation Workflow

To explore all four knowledge-shift archetypes end-to-end in the dashboard using real Wikipedia events from the curated historical lake:

1. **Print the 4 Case Studies in Your Terminal:**
   ```bash
   cd Repo
   ./run-demo.sh --walkthrough-only
   ```
   This displays the exact article titles, window timestamps, detector scores, additive feature points, human-readable reasons, spillover neighbors, diff snippets, and geospatial coordinates for four distinct alert types:
   - **Case Study #1 (`CONFIRMED_NEWS`):** High multi-editor velocity surge corroborated by $> 2.5\times$ daily pageviews and low revert rate.
   - **Case Study #2 (`BREAKING_NEWS_CANDIDATE`):** Rapid multi-editor surge on a quiet/moderate page prior to next-day pageview publication.
   - **Case Study #3 (`EDIT_WAR_CONTROVERSY`):** Intense back-and-forth reversion dispute (`revert_ratio >= 30%`, high `rewriting_bytes`) separated from breaking news.
   - **Case Study #4 (`MAJOR_REWRITE`):** Focused 1–2 editor structural expansion/overhaul (`gross_bytes >= 3,500 B`) missed by static editor-count rules.

2. **Trace Any Case Study Across the Dashboards (`http://127.0.0.1:8080`):**
   - **Step A (`/` Live Pulse):** Locate the article in the **Alert Feed** or switch **SOURCE** to `Stored activity`, pick the alert's date, and observe the spike on the activity and byte churn timelines.
   - **Step B (`/investigation` Page Investigation):** Click the article title to inspect its 5-component score breakdown (`feature_contributions`), read the exact added/deleted text in the **Revision Diff** cards, and check the **Lift Wing Revert Risk** scores for revisions during that window.
   - **Step C (`/contextual-features` & `/geospatial-map`):** Inspect which linked/backlinked neighbor articles were co-edited during the same window (`co_edited_in_window = True`) and check the article subject's resolved coordinates and provenance (`wikipedia_coord` or `wikidata_p276_p17`).
   - **Step D (`/historical` Historical Intelligence):** Compare the article's anomaly window against its 47-day historical baseline profile and daily surge rank.
