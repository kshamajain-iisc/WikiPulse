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

## 2. Open Items for Next 2 Days Fine-Tuning

### Item A (Medium Priority): Rename `REJECTED RECORDS` Card on `/system` Dashboard in Offline Mode
- **Files:**
  - [Repo/dashboards/data_service.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/dashboards/data_service.py#L137-L147)
  - [Repo/dashboards/pages/system.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/dashboards/pages/system.py#L31-L34)
- **Current Behavior:**
  - When running in offline/backup demo mode (`./run-demo.sh`), `data_service.py` maps `quality["invalid_records"]` to `total_raw_events - curated_deduplicated_events` (`7,251,442` filtered `categorize` events).
  - `system.py` displays this count under the card **`REJECTED RECORDS` ("Includes malformed test messages")** and labels **`RAW RECORDS`** as `"Current / prior UTC ingestion day"`.
- **Planned Fine-Tuning:**
  - Update the card subtitle/label when loaded from `ingestion_summary.json` so filtered `categorize` noise (`41.6%` of raw feed) is clearly distinguished from malformed records (`0` malformed, `100%` acceptance of valid `edit`/`new`/`log` events).

---

### Item B (Medium Priority): Surface Pageview Baselines & Update Pageview Banner on `/historical` Dashboard
- **File:** [Repo/dashboards/pages/historical.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/dashboards/pages/historical.py#L21-L45)
- **Current Behavior:**
  - Line 44 displays a static notice: *"Pageviews are delayed supporting evidence. No view counts or correlations are shown until a normalized pageview source is connected."*
  - Lines 22–24 (`baseline_columns`) show `title`, `activity_tier`, `avg_daily_edits`, `last_available_date`, and `has_missing_data`, omitting `avg_daily_pageviews`, `last_pageview_date`, and `pageview_missing_flag`.
- **Planned Fine-Tuning:**
  - Add `("Mean daily views", "avg_daily_pageviews")`, `("Last pageview date", "last_pageview_date")`, and `("Views missing", "pageview_missing_flag")` to `baseline_columns` in `historical.py`, and update the bottom notice to reflect the connected `Repo/data/analytics/` pageview series.

---

### Item C (Medium Priority): Align Component Weight Headers on `/investigation` Dashboard
- **Files:**
  - [Repo/dashboards/pages/investigation.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/dashboards/pages/investigation.py#L19-L25)
  - [Repo/src/dashboard_queries.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/dashboard_queries.py#L81-L85)
- **Current Behavior:**
  - Detector B in [Repo/src/anomaly/scoring_sparkml.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/anomaly/scoring_sparkml.py) uses component weights `35` (velocity) / `25` (editors) / `20` (byte churn) / `10` (reverts/consensus) / `10` (pageviews) = `100`.
  - `investigation.py` labels the editor column `("Editors / 20", "editor_points")` instead of `Editors / 25`, and omits `pageview_points` (`Pageviews / 10`).
- **Planned Fine-Tuning:**
  - Update the header in `investigation.py` to `Editors / 25` and expose `pageview_points` (`Pageviews / 10`) in `dashboard_queries.py` and `investigation.py`.

---

### Item D (Low–Medium Priority): Unify Baseline Lookup in Live Streaming Detector (`src/detection/`)
- **Files:**
  - [Repo/src/detection/__main__.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/detection/__main__.py)
  - [Repo/src/detection/pipeline.py](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/src/detection/pipeline.py#L17-L22)
- **Current Behavior:**
  - When `./run-local.sh` starts `python -m src.detection stream`, it looks for `data/week2/historical/baselines_5m` (generated by `src/prepare_history.py`) rather than falling back to `curated/page_baselines` (generated by `src/historical/build_baselines.py`).
- **Planned Fine-Tuning:**
  - Allow `load_baselines()` in `src/detection/pipeline.py` to fall back to `curated/page_baselines` (scaling `avg_hourly_edits` and `std_hourly_edits` to `5m`/`1h`) when `data/week2/historical/baselines_{duration}` is absent.

---

### Item E (Low Priority): Update Documentation Summary Tables from 46-Day Numbers to Final 55-Day Numbers
- **Files:**
  - [Repo/README.md](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/README.md)
  - [Repo/DEMO_COMMANDS.md](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/DEMO_COMMANDS.md)
  - [Repo/docs/final_project_report.md](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/docs/final_project_report.md)
  - [Repo/docs/presentation_and_demo_guide.md](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/docs/presentation_and_demo_guide.md)
- **Current Behavior:**
  - Markdown summary tables still reference the earlier 46-day run (`14,398,713` raw $\rightarrow$ `8,428,021` curated rows, `2026-09-15 .. 2026-10-30`) while the curated Parquet lake now spans **55 days (`2026-09-15 .. 2026-11-08`, `17,441,561` raw $\rightarrow$ `10,190,119` curated)**.
- **Planned Fine-Tuning:**
  - Update the metric tables in `README.md`, `DEMO_COMMANDS.md`, `final_project_report.md`, and `presentation_and_demo_guide.md` to reflect the synchronized 55-day numbers.

