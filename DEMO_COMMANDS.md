# WikiPulse — Quick Commands for Presentation & Demo

**Project:** WikiPulse — Real-Time Detection and Analysis of Wikipedia Knowledge Shifts  
**Course:** DA231o — Data Engineering at Scale (IISc)

---

## 1. One-Command Demo Launcher (`./run-demo.sh`)

From `Repo/` or the project root (`Project-WikiPulse/`):

```bash
# Launch the pre-warmed 5-screen Dash + Plotly Backup Demo Server on http://127.0.0.1:8080
./run-demo.sh

# Print the 4 real Wikipedia Knowledge-Shift case studies in the terminal only
./run-demo.sh --walkthrough-only

# Run full pre-flight verification (11 Parquet tables + 11 benchmarks + 5 HTTP routes)
./run-demo.sh --verify-only

# Launch on a custom port (e.g., 8081)
./run-demo.sh --port 8081
```

---

## 2. Direct Python Commands for Presentation Demo

### A. Offline / Curated Lake Backup Demo (Zero Internet or Docker Needed)
```bash
# 1. Print the 4 real Wikipedia case studies (Confirmed News, Candidate, Edit War, Major Rewrite)
.venv/bin/python3 Repo/src/demo/backup_demo.py --walkthrough-only

# 2. Verify all 11 Curated Parquet tables, 11 Benchmark JSONs & 5 Dash HTTP routes (< 2.0s latency)
.venv/bin/python3 Repo/src/demo/backup_demo.py --verify-only

# 3. Launch the pre-warmed interactive Dash + Plotly application at http://127.0.0.1:8080
.venv/bin/python3 Repo/src/demo/backup_demo.py --port 8080
```

### B. Live Streaming Stack (Docker Kafka + EventStreams Collector + Spark Streaming)
```bash
# Prerequisite checks only (Docker, Java 17, port, .env User-Agent)
cd Repo && ./run-local.sh --check

# Start Kafka, collector, Spark streaming windows (5m/1h), detectors & Dash UI
cd Repo && ./run-local.sh --port 8080
```

### C. Automated Unit & Integration Test Suite (45 Tests across Milestones 1–8)
```bash
PYTHONPATH=Repo .venv/bin/python3 -m unittest discover -s Repo/tests -v
```

---

## 3. 5-Screen Browser Tour (`http://127.0.0.1:8080`)

| Screen | URL Route | What to Show During Demo |
| :--- | :--- | :--- |
| **1. Live & Replay Radar** | `http://127.0.0.1:8080/` | Toggle `5m` / `1h` durations; inspect alert badges (`CONFIRMED_NEWS`, `BREAKING_NEWS_CANDIDATE`, `EDIT_WAR_CONTROVERSY`, `MAJOR_REWRITE`), `historical_score`, and $\ge 2$ human-readable `reasons`. |
| **2. Article Investigation** | `http://127.0.0.1:8080/investigation` | Select an alerted article (e.g., *Navi Pillay*, *Katie Zacharia*, *Mike Ditka*) to inspect additive `feature_contributions` points, MediaWiki `action=compare` **Revision Diffs**, and **Lift Wing Revert Risk** vs. 48h actual reverts. |
| **3. Historical & Churn** | `http://127.0.0.1:8080/historical` | Show 46-day Human vs. Bot split, Gross Churn ($\sum |\Delta|$), Net Growth ($\sum \Delta$), Rewriting Volume ($\sum |\Delta| - |\sum \Delta|$), and data freshness (`last_available_date = 2026-10-30`). |
| **4. Graph & Subject Map** | `http://127.0.0.1:8080/relationships` | Show the **Related-Page Link/Backlink Spillover Graph** (`607` edges, `195` co-edited in window) and the **Global Article-Subject Map** (`121` located via Wikipedia/Wikidata, `0` editor IPs). |
| **5. Engineering & Scale** | `http://127.0.0.1:8080/system` | Show **Ingestion Completeness** (`14.40M` raw $\rightarrow$ `8.43M` accepted, `100%`), **Strong Scaling Curves**, **Streaming Load vs. Capacity (`1x..100x`)**, and **Stop/Restart Backlog Recovery (`0` backlog, `0` duplicates)**. |

