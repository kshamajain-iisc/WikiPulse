# WikiPulse — Historical Data Setup & Curated Ingestion Guide

This guide explains how to download the collected English Wikipedia historical dataset from Google Drive, place it in your local repository, and run `src/historical/ingest_curate.py` to build the partitioned Curated Parquet Lake.

---

## 1. Download the Historical Dataset from Google Drive

The collected historical dataset is synced to the shared Google Drive folder below:

- **Google Drive Link:** [WikiPulse Historical Data (`historical/data`)](https://drive.google.com/drive/folders/1giibBLj6IKZI-bdIX65DtQvdzAEwYXCt?usp=drive_link)

### What Is Inside the Shared Drive Folder
| Subfolder | Contents | Description |
| :--- | :--- | :--- |
| **`enwiki_recentchanges/`** | `rc_YYYY-MM-DD.jsonl.gz`, `checkpoints/`, `runs.jsonl`, `collector.log` | Daily compressed JSONL archives of English Wikipedia `recentchanges` (`2026-09-15` to `2026-11-08`, 55 days, ~1.18 GB, 17.44M raw events). |
| **`analytics/`** | `edits_daily_*.json`, `bytes_abs_*.json`, `bytes_net_*.json`, `pageviews_top_daily.json` | Wikimedia REST Analytics API daily project-wide and per-article series used for baseline joins and pageview corroboration. |
| **`enwiki_revisions/`** | `rev_YYYY-MM-DD.jsonl.gz`, `checkpoints/`, `runs.jsonl` | Historical revision metadata snapshots. |
| **`curated/`** *(optional)* | 11 Parquet tables + `benchmarks/*.json` | Pre-built curated Parquet tables if you want to run `./run-demo.sh` immediately without re-running Spark jobs. |

---

## 2. Move Downloaded Data into Your Repository

`src/config/settings.py` automatically resolves the data directory in the following priority order:
1. `WIKIPULSE_DATA_DIR` environment variable (if set).
2. Sibling `../historical/data/` folder (if present outside `Repo/`).
3. Repository-local `data/` folder (`Repo/data/` — **default for standard repo clones**).

### Directory Placement (Standard Clone)
After downloading from Google Drive, move the `rc_*.jsonl.gz` files (along with `checkpoints/` and `runs.jsonl`) into **`data/enwiki_recentchanges/`** inside your repository root (`Repo/data/enwiki_recentchanges/`):

```bash
# Move the downloaded enwiki_recentchanges files into data/enwiki_recentchanges/:
mkdir -p data/enwiki_recentchanges data/analytics data/enwiki_revisions data/curated
mv ~/Downloads/enwiki_recentchanges/* data/enwiki_recentchanges/
```

> [!NOTE]
> You do not need to manually download `analytics/` — running `src/historical/build_baselines.py` automatically creates and populates `Repo/data/analytics/` (including `pageviews_top_daily.json`, `edits_daily_*.json`, `editors_daily.json`, and `pages/views_*.json`).

Your folder structure inside the repository should look like this:

```text
Repo/
├── data/
│   ├── analytics/
│   ├── curated/                  # Populated by ingest_curate.py and downstream jobs
│   ├── enwiki_recentchanges/
│   │   ├── checkpoints/
│   │   ├── rc_2026-09-15.jsonl.gz
│   │   ├── rc_2026-09-16.jsonl.gz
│   │   ├── ...
│   │   ├── rc_2026-11-08.jsonl.gz
│   │   └── runs.jsonl
│   └── enwiki_revisions/
├── dashboards/
├── src/
│   ├── config/
│   │   └── settings.py
│   └── historical/
│       └── ingest_curate.py
└── run-demo.sh
```

---

## 3. Run `ingest_curate.py` to Convert Raw JSONL.gz into Curated Parquet

Once the `rc_*.jsonl.gz` files are in `data/enwiki_recentchanges/`, run `src/historical/ingest_curate.py` to convert the raw JSONL archives into the partitioned Parquet table at `data/curated/rc_events/`.

### Command (Run from `Repo/` root)
```bash
PYTHONPATH=. .venv/bin/python3 src/historical/ingest_curate.py
```

*(Or if running from the parent `Project-WikiPulse/` workspace root:)*
```bash
.venv/bin/python3 Repo/src/historical/ingest_curate.py
```

### Optional CLI Flags
| Flag | Purpose |
| :--- | :--- |
| `--clean-raw-overlap` | Deduplicates raw `rc_*.jsonl.gz` files in-place by `rcid` and repairs any interrupted multi-member gzip streams before Spark ingestion. |
| `--days 2026-09-26,2026-09-27` *(or `--days all`)* | Specifies which daily `.jsonl.gz` files to deduplicate in-place when `--clean-raw-overlap` is passed. |
| `--skip-parquet` | Runs only the raw `.jsonl.gz` cleanup/audit without running the Spark Parquet write. |

### Expected Terminal Output (55-Day Dataset)
```text
============================================================================
 WIKIPULSE MILESTONE 4.1: DATA CLEANUP & CURATED PARQUET INGESTION
============================================================================

[Step 2] Running Spark ingestion across all historical/data/enwiki_recentchanges/rc_*.jsonl.gz ...
  • Total raw events read (incl. categorize) : 17,441,561
  • Valid edit/new/log events (pre-dedup)    : 10,190,119
  • Curated deduplicated events in Parquet   : 10,190,119 across 55 days
  • Raw JSONL.gz size vs Curated Parquet     : 1181.71 MB -> 1220.96 MB
  • Completed in                             : 614.3s
  • Output path                              : .../data/curated/rc_events
```

---

## 4. What `src/historical/ingest_curate.py` Does Under the Hood

`src/historical/ingest_curate.py` implements **Milestone 4.1 (Historical Data Cleanup, Deduplication & Curated Parquet Ingestion)**:

1. **Multi-Member Gzip Recovery & In-Place Raw Deduplication (`iter_recoverable_gz_lines` & `deduplicate_raw_gz_file`):**
   - Because the collector appends to daily `.jsonl.gz` files across multiple resumable runs, an interrupted run can leave a multi-member gzip file with a truncated trailing stream.
   - When `--clean-raw-overlap` is used, the script scans raw gzip member headers (`\x1f\x8b\x08`), salvages every valid JSON line across all gzip members, and removes duplicate `rcid` records caused by overlapping collection windows.

2. **Single-Pass Raw vs. Valid Event Audit:**
   - Reads all `rc_*.jsonl.gz` files into PySpark and counts total raw records (`17,441,561`) versus non-categorize records (`10,190,119`) to verify the Proposal's $\ge 99\%$ valid event acceptance requirement.

3. **Noise Filtering & Global `rcid` Deduplication:**
   - Filters out MediaWiki `categorize` events (~41.6% of the raw feed), retaining only meaningful `edit`, `new`, and `log` events.
   - Runs `.dropDuplicates(["rcid"])` across the entire multi-day dataset so any event spanning day boundaries or overlapping checkpoints is stored exactly once.

4. **Canonical Schema Normalization (`normalize_recentchanges_df` $\rightarrow$ `RAW_EDIT_SCHEMA`):**
   - Normalizes raw MediaWiki fields into the strict 23-column PySpark `RAW_EDIT_SCHEMA` defined in `src/models/schemas.py`:
     - Parses ISO-8601 `timestamp` into UTC `ts` (`TimestampType`), `day` (`DateType`), and `hour` (`TimestampType`).
     - Extracts `old_revid` and `revid`, and computes signed byte change (`delta = newlen - oldlen`) and absolute byte churn (`abs_delta = |newlen - oldlen|`).
     - Identifies anonymous and MediaWiki temporary accounts (`~2026-...` prefix or `anon`/`temp` flags) as `is_anon`.
     - Flags reverts (`is_revert = True`) whenever MediaWiki tags contain `mw-reverted`, `mw-rollback`, `mw-undo`, or `mw-manual-revert`.

5. **Atomic Partitioned Parquet Write & Benchmark Logging:**
   - Repartitions the normalized DataFrame into 16 partitions by `day` and writes partitioned Parquet (`day=YYYY-MM-DD/`) to a clean local `/tmp` staging directory first before moving it into `data/curated/rc_events/` (preventing macOS Finder `Icon\r` files from breaking Spark's `_temporary` commit phase).
   - Verifies the written Parquet row count and saves ingestion metrics (`total_raw_events`, `curated_deduplicated_events`, `distinct_days`, `raw_jsonl_gz_mb`, `curated_parquet_mb`, `ingestion_runtime_sec`) to `data/curated/benchmarks/ingestion_summary.json`.

---

## 5. Global Baseline vs. Evaluation Date Split (`src/config/settings.py`)

Across the 55-day collected dataset (`2026-09-15` to `2026-11-08`), the global split between historical baseline training and anomaly evaluation is centrally defined in `src/config/settings.py`:

| Constant | Default Date | Span | Purpose |
| :--- | :--- | :--- | :--- |
| **`BASELINE_START_DATE`** | `2026-09-15` | 47 days (`2026-09-15` .. `2026-10-31`) | Start of the historical baseline window used by `src/historical/build_baselines.py` |
| **`BASELINE_END_DATE`** | `2026-10-31` | 47 days (`1,128` UTC hours) | End of the historical baseline window (`curated/page_baselines` & `Repo/data/analytics/`) |
| **`EVALUATION_START_DATE`** | `2026-11-01` | 8 days (`2026-11-01` .. `2026-11-08`) | Start of the post-baseline anomaly evaluation window (`src/anomaly/window_features.py`) |
| **`EVALUATION_END_DATE`** | `2026-11-08` | 8 days (`192` UTC hours) | End of the post-baseline anomaly evaluation window |

---

## 6. Next Steps After Running `ingest_curate.py`

Once `data/curated/rc_events/` is created, you can run the downstream batch pipeline or launch the interactive demo:

```bash
# Build per-page historical baselines over 2026-09-15 .. 2026-10-31 (M4.2) & churn/bot rankings (M4.3)
# (Also automatically populates Repo/data/analytics/ through 2026-10-31)
PYTHONPATH=. .venv/bin/python3 src/historical/build_baselines.py
PYTHONPATH=. .venv/bin/python3 src/historical/rankings_benchmark.py

# Build 5m & 1h window features over 2026-11-01 .. 2026-11-08 (M5.1), run detectors (M5.2) & audit (M5.3)
PYTHONPATH=. .venv/bin/python3 src/anomaly/window_features.py
PYTHONPATH=. .venv/bin/python3 src/anomaly/scoring_sparkml.py
PYTHONPATH=. .venv/bin/python3 src/anomaly/evaluate_alerts.py

# Or launch the turnkey presentation demo directly on http://127.0.0.1:8080:
./run-demo.sh --port 8080
```

