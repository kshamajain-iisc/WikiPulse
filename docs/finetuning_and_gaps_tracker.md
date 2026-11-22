# WikiPulse — Post-Implementation Fine-Tuning & Gaps Tracker

This document tracks the remaining UI polish, streaming-to-curated bridge, and documentation synchronization items identified during the full Milestones 1–8 audit.

---

## 1. Completed / Synchronized Item

- [x] **Gap #1 — Synchronize Milestones 4, 5 & 6 Curated Parquet Tables & Benchmark JSONs with the 55-Day Dataset (`2026-09-15` .. `2026-11-08`):**
  - Global split set in `Repo/src/config/settings.py`:
    - `BASELINE_START_DATE = "2026-09-15"`, `BASELINE_END_DATE = "2026-10-31"` (47 days)
    - `EVALUATION_START_DATE = "2026-11-01"`, `EVALUATION_END_DATE = "2026-11-08"` (8 days)
  - Re-ran Milestone 4 (`ingest_curate.py`, `build_baselines.py`, `rankings_benchmark.py`), Milestone 5 (`window_features.py`, `scoring_sparkml.py`, `evaluate_alerts.py`), Milestone 6 (`graph_and_diffs.py`, `geospatial_map.py`, `revert_risk_monitor.py`, `dashboard/app.py --benchmark-only`), and Milestone 8 (`backup_demo.py --verify-only`) so all 11 Curated Parquet tables and benchmark JSONs are in sync.

---

## 2. Completed Fine-Tuning Items

- [x] **Item A (Medium Priority): Clarify `ACCEPTED EDITS` / `FILTERED EVENTS` Cards on `/system` Dashboard in Offline Mode**
  - Updated `Repo/dashboards/data_service.py` and `Repo/dashboards/pages/system.py` so `ingestion_summary.json` displays `100% valid edit/new/log accepted (58.42% of raw)` and `Filtered categorize noise (0 malformed records)` across the 55-day curated dataset.
- [x] **Item B (Medium Priority): Surface Pageview Baselines & Update Pageview Banner on `/historical` Dashboard**
  - Added `("Mean daily views", "avg_daily_pageviews")`, `("Last pageview date", "last_pageview_date")`, and `("Views missing", "pageview_missing_flag")` to `baseline_columns` in `Repo/dashboards/pages/historical.py` and updated the pageview confirmation notice to reflect the connected `Repo/data/analytics/` series.
- [x] **Item C (Medium Priority): Align Component Weight Headers on `/investigation` Dashboard**
  - Updated `Repo/dashboards/pages/investigation.py` to label `("Editors / 25", "editor_points")` and exposed `("Pageviews / 10", "pageview_points")` in `Repo/src/dashboard_queries.py` and `investigation.py` so all 5 additive components (`35 + 25 + 20 + 10 + 10 = 100`) are displayed.
- [x] **Item D (Low–Medium Priority): Unify Baseline Lookup in Live Streaming Detector (`src/detection/`)**
  - Updated `Repo/src/detection/__main__.py` and `Repo/src/detection/pipeline.py` so `load_baselines()` falls back to `curated/page_baselines` and adapts hourly/daily baseline moments to `5m`/`1h` windows when `data/week2/historical/baselines_{duration}` is absent.
- [x] **Item E (Low Priority): Update Documentation Summary Tables from 46-Day Numbers to Final 55-Day Numbers**
  - Updated `Repo/README.md`, `Repo/DEMO_COMMANDS.md`, `Repo/docs/final_project_report.md`, and `Repo/docs/presentation_and_demo_guide.md` to reflect the synchronized 55-day curated lake (`17,441,561` raw $\rightarrow$ `10,190,119` curated events, `2026-09-15 .. 2026-11-08`).
- [x] **Item F: Fix `run-demo.sh` & `run-local.sh` Virtualenv Resolution, Supervisor State Paths & Curated Window Horizon**
  - Updated `Repo/run-demo.sh` and `Repo/run-local.sh` to automatically resolve and verify a working `.venv` (including the parent project `.venv` when `Repo/.venv` is incomplete).
  - Updated `Repo/src/run_local.py` so `write_supervisor_state()` dynamically resolves `ROOT` and creates `data/run` before writing, created `Repo/.env` with a valid `WIKIMEDIA_USER_AGENT` and dynamic `SPARK_KAFKA_PACKAGE` matching the installed PySpark version, and anchored curated finalized windows in `Repo/src/dashboard_queries.py` and `Repo/src/demo/backup_demo.py` to the latest evaluation window timestamp.


