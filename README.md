# WikiPulse — Real-Time Detection and Analysis of Wikipedia Knowledge Shifts

**Authors:** Kshama Jain, Senthil Raj K, Pothukanuri Sai Venkat, Akash H S (Team QuadCore)  
**Course:** DA231o — Data Engineering at Scale (IISc)

WikiPulse is an end-to-end distributed data platform that detects anomalous Wikipedia editing activity in real time and over historical archives, normalizes bursts against per-page historical baselines, distinguishes breaking news from edit wars and major rewrites, and enriches alerts with related-page topic graphs, revision diffs, privacy-preserving article-subject geospatial maps, and Wikimedia Lift Wing Revert Risk scoring.

---

## 1. Quickstart: Turnkey Demo, Live Stack & Full Pipeline Commands

### A. 1-Command Reproducible Backup Demo (Recommended for Presentation)
Runs directly off the verified **46-day Curated Parquet Lake** (`historical/data/curated/`, **8,428,021** deduplicated events across **11 Parquet tables**) with zero external network or Docker dependencies:

```bash
# 1. Run pre-flight readiness verification + 4 real Wikipedia case-study walkthrough:
.venv/bin/python3 Repo/src/demo/backup_demo.py --verify-only

# 2. Print only the 4 real Wikipedia case studies in the terminal:
.venv/bin/python3 Repo/src/demo/backup_demo.py --walkthrough-only

# 3. Launch the pre-warmed 5-screen Dash + Plotly application on http://127.0.0.1:8080:
.venv/bin/python3 Repo/src/demo/backup_demo.py --port 8080
```

### B. Run the Full Automated Test Suite (Milestones 1–8)

```bash
PYTHONPATH=Repo .venv/bin/python3 -m unittest discover -s Repo/tests -v
```

### C. Run the Live Streaming Stack (Docker Kafka + EventStreams Collector + Spark Streaming)

Start your Docker-compatible container engine, ensure `WIKIMEDIA_USER_AGENT` is set in `.env`, then run from `Repo/`:

```bash
cd Repo && ./run-local.sh --port 8080
```

This starts Kafka, creates the topics (`wiki.raw.edits` and `wiki.shift.candidates`), and supervises the live collector, Spark Bronze ingestion, watermarked 5-minute and 1-hour window aggregators, streaming detectors, and the Dash + Plotly frontend at `http://127.0.0.1:8080`.

---

## 2. Architecture & Milestone Execution Pipeline (Milestones 1–8)

```bash
# Milestone 1: Verify APIs, 8 Shared PySpark Schemas & Normalization
.venv/bin/python3 Repo/src/verify_milestone1.py

# Milestone 4.1: Ingest & Curate 46-Day Partitioned Parquet Lake (14.40M raw -> 8.43M curated)
.venv/bin/python3 Repo/src/historical/ingest_curate.py

# Milestone 4.2: Build Per-Page Historical Baselines (1.80M pages) & Analytics Pageview Join
.venv/bin/python3 Repo/src/historical/build_baselines.py

# Milestone 4.3: Build Churn/Bot Summaries, Daily Rankings & Run Manual/RDD/Parquet Benchmarks
.venv/bin/python3 Repo/src/historical/rankings_benchmark.py

# Milestone 5.1: Build 5-Minute (350k) & 1-Hour (190k) Page Window Features
.venv/bin/python3 Repo/src/anomaly/window_features.py

# Milestone 5.2: Run Detectors A (Fixed), B (Historical Score) & C (Spark ML) -> curated/alerts
.venv/bin/python3 Repo/src/anomaly/scoring_sparkml.py

# Milestone 5.3: Run Synthetic Pattern Injection, False-Alert & Replay Idempotency Audit
.venv/bin/python3 Repo/src/anomaly/evaluate_alerts.py

# Milestone 6.1: Build Related-Page Link/Backlink Spillover Graph & Revision Compare Diffs
.venv/bin/python3 Repo/src/enrichment/graph_and_diffs.py

# Milestone 6.2: Build 2-Tier Wikipedia/Wikidata Subject Geospatial Map (No Editor IPs)
.venv/bin/python3 Repo/src/enrichment/geospatial_map.py

# Milestone 6.3: Run Lift Wing Revert Risk Scoring & 48h Revert Threshold Calibration
.venv/bin/python3 Repo/src/enrichment/revert_risk_monitor.py

# Milestone 6.4: Benchmark All 11 Curated Tables & 4 Proposal Dashboard Views (< 2.0s)
.venv/bin/python3 Repo/src/dashboard/app.py --benchmark-only

# Milestone 7.1: Execute Strong/Weak Scaling, Shuffle Partition Sweep & Input-Size Benchmarks
.venv/bin/python3 Repo/src/benchmarks/scaling_experiments.py

# Milestone 7.2: Execute Multi-Speed Streaming Load, Window Latency & Stop/Restart Recovery Tests
.venv/bin/python3 Repo/src/benchmarks/load_and_recovery_benchmark.py
```

---

## 3. Summary of Verified Engineering & Feature Results

| Milestone / Dimension | Measured Result |
| :--- | :--- |
| **Data Ingestion & Hygiene (M4.1)** | **14,398,713** raw events (`2026-09-15 .. 2026-10-30`, 46 days) $\rightarrow$ **8,428,021** valid `edit`/`new`/`log` events (**100% acceptance rate**, $\ge 99\%$ target). Multi-member gzip recovery salvaged all `269,017` rows of `2026-10-27`. |
| **Manual Sample Audit (M4.3)** | **100% exact match** between pure-Python stdlib and Spark DataFrame on 2,000 events (`gross_churn = 2,024,277 B`, `rewriting_bytes = 1,179,254 B`). |
| **Multi-Detector Accuracy (M5.2–M5.3)** | **100% synthetic recall (`5/5`)** with $\ge 2$ human-readable reasons on 100% of alerts (`avg 4.03`). False-alert rate reduced from **68.81%** (Detector A Fixed Rule) to **10.72%** (Detector B Historical Score) and **0.29%** (Detector C Spark ML). |
| **Topic Graph & Revision Diffs (M6.1)** | **607** directed link/backlink edges (**195 co-edited spillover edges, 32.13%**); **100/100 (100.0%)** revision diffs return usable change data ($\ge 90\%$ target). |
| **Subject Geospatial Map (M6.2)** | **121 / 160 (75.62%)** alerts resolved via Wikipedia coordinates & Wikidata (`P625`, `P276`, `P17`) with source/quality tags; **39 (24.38%)** preserved as `unlocated`; **0 editor IPs used**. |
| **Lift Wing Revert Risk (M6.3)** | **74.42%** routine edits pre-filtered; **100.0%** valid score coverage (`92.5%` live Lift Wing ML + `7.5%` heuristic fallback, $\ge 95\%$ target). Calibrating $\tau = 0.65$ improves `composite_risk` F1 from `68.57%` to **`76.92%`**; median 48h revert MTTR = **`66.5s`**. |
| **Scaling & Shuffle Benchmarks (M7.1)** | Strong scaling reaches **`1.69x` speedup** on `local[4]`; shuffle partition sweep proves **`16` partitions (`10.00s`)** is globally optimal; full 46-day lake (`8.43M` rows) runs in **`38.09s` (`221,295 rows/s`)**; DataFrame outperforms RDD by **`2.03x–2.86x`**. |
| **Streaming Load & Failure Recovery (M7.2)** | Processing capacity (`2,266–8,989 rows/s`) exceeds `1x..100x` input rates; closed 5m windows finalize in **`0.35s–9.45s` (`< 60s` target)**; controlled stop/restart drains **`123,537` backlog events $\rightarrow$ `0` in `13.7s–18.7s`** with **`0` duplicate alerts** (`39/39` reference parity). |
| **Dashboard Query Latency (M6.4 & M8.1)** | All 4 Proposal Dashboard Views execute in **`0.006s – 0.889s`** over stored Parquet (`< 2.0s` target, ~14.5x faster than raw JSONL.gz). |

---

## 4. Key Documentation

- **[Historical Data Setup & Curated Ingestion Guide](DATA_SETUP.md)** — Google Drive download link, `Repo/data/enwiki_recentchanges` placement, and `ingest_curate.py` workflow.
- **[Final Project Report](docs/final_project_report.md)** — Complete architecture, methodology, and evaluation across all 6 Proposal Experiments and 14 Metrics.
- **[Presentation & Live/Backup Demo Guide](docs/presentation_and_demo_guide.md)** — 12-slide presentation deck outline and 5-minute guided demo runbook.
- **[Implementation Review (Milestones 1–8)](docs/implementation_review_milestones_1_and_4.md)** — Detailed file-by-file inventory, design rationale, and measured artifacts.
- **[Milestone 1 Technical Specification](docs/milestone1_specs_and_models.md)** — Confirmed Wikimedia APIs, 8 shared data models, and 4 dashboard specs.
- **[Memory Sizing & Capacity Planning](docs/memory_and_capacity_planning.md)** — JVM heap, shuffle partition, and unified memory sizing guide.
- **[Week 2 Runbook](docs/week2.md)** & **[Week 3 Runbook](docs/week3.md)** — Streaming ingestion, replay, and local launcher details.
