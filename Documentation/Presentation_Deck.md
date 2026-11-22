# WikiPulse: Real-Time Detection and Analysis of Wikipedia Knowledge Shifts — Presentation Slide Deck

**Authors:** Kshama Jain, Senthil Raj K, Pothukanuri Sai Venkat, Akash H S (Team QuadCore)  
**Course:** DA231o — Data Engineering at Scale, Indian Institute of Science (IISc), 2026  
**Companion Files:** [Interactive HTML Slide Deck](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/Documentation/WikiPulse_Final_Presentation.html) · [PowerPoint (.pptx) Deck](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/Documentation/WikiPulse_Final_Presentation.pptx) · [Project Documentation](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/Documentation/Project_Documentation.md) · [Technical Documentation](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/Documentation/Technical_Documentation.md) · [User Guide](file:///Users/senthilrajk/Downloads/002_IISc_DSBA/Data%20Engineering%20at%20Scale/Project-WikiPulse/Repo/Documentation/User_Guide.md)

---

## Slide 1 — Cover Slide: Project Name & Contributors

### WikiPulse: Real-Time Detection and Analysis of Wikipedia Knowledge Shifts
- **Core Stack Pills:** Apache Kafka (KRaft) · Apache Spark (SQL & ML) · Partitioned Parquet & PyArrow · Plotly Graph Objects & Dash (Network & Geo Maps)
- **Contributors (Team QuadCore):** Kshama Jain · Senthil Raj K · Pothukanuri Sai Venkat · Akash H S

---

## Slide 2 — Project Introduction & 67-Day Scale

### WikiPulse: Turning Live Wikipedia Edits into Explainable Event Intelligence
- **67-Day Scale (`2026-09-15` to `2026-11-20`):** `21,454,187` raw events → `12,773,366` curated valid events (`100%` retention) · `5.83 GB` gross text churn (`60.21%` rewriting) · `3,307,257` profiled pages · `10,631` union alerts.
- **3 Core Pillars:** (1) Real-Time + Replay Observatory, (2) Explainable Multi-Signal Detection, (3) Contextual Investigation Suite.

---

## Slide 3 — End-to-End Production Technology Stack

### Distributed Streaming, Compute, ML, Lakehouse & Graph Visualization Stack
1. **Ingestion:** Wikimedia EventStreams SSE · Apache Kafka (KRaft) · `1x..100x` Replay Engine.
2. **Compute:** Apache Spark (`pyspark.sql` Catalyst DataFrames, Structured Streaming, MediaWiki graph edge assembly).
3. **ML & Risk:** `pyspark.ml` (`VectorAssembler` → `StandardScaler` → `KMeans` → `RandomForest`) + Wikimedia Lift Wing.
4. **Lakehouse:** 11 Day-Partitioned Snappy Parquet Tables + PyArrow zero-copy reader + SHA-256 disk cache.
5. **UI & Graph Visualization:** Dash + **Plotly `graph_objects`** (`Scatter` hub-and-spoke spillover network + `Scattergeo` geospatial maps) across 8 interactive screens.

---

## Slide 4 — High-Level System Design & Architecture

### 5 Decoupled Tiers (Streaming + Batch)
1. **Sources:** EventStreams SSE · 67-Day Archives (`21.45M`) · Enrichment APIs (Pageviews, Links/Diffs, Wikidata, Lift Wing).
2. **Ingestion:** SSE & Replay → Apache Kafka (KRaft) · Batch Hygiene (`12.77M` Curated Rows).
3. **Apache Spark:** Structured Streaming (`5m`/`1h`) · Baselines & Churn SQL · Anomaly & Spark ML · Parquet Lakehouse (`11` Tables).
4. **Enrichment:** Spillover Graph · Compare Diffs · Subject Geo Map · Lift Wing Risk.
5. **Serving UI:** PyArrow Reader (`< 1.35s`) → Dash + Plotly (`8` Interactive Screens).

---

## Slide 5 — Live Stream (`Screen 01: Live Pulse /`)

- Real-time Wikimedia SSE → Apache Kafka (KRaft) → Spark Structured Streaming (`1m` live preview + `10m`-watermark `5m`/`1h` windows + `1x..100x` replay).
- Live snapshot (`01_live_pulse.png`): `205` Article Edits · `173` Active Articles · `39.0k B` · `97.6%` Human Activity · `1.4s` refresh SLA.

---

## Slide 6 — Page Inspection (`Screen 02: /investigation`)

- **1. *2026 US Senate elections* (`02_page_investigation.png`):** `313` edits · `99.4%` human · `52.9k B` · additive knowledge-shift evidence table + observed edits vs. historical `μ + 2σ` baseline band.
- **2. *2026 Microsoft outage* (`10_page_inspection.png`):** `77` edits · `100.0%` human · `14.6k B` · live hourly activity bursts, gross vs. net content movement, and contributor balance.

---

## Slide 7 — Historical Data (`Screen 03: /historical`)

- **67-Day Processing Funnel:** `21.45M` raw → `12.77M` curated (`1.51 GB` Parquet) → `7.83M` Mainspace → `3.31M` profiled pages → `1.50M` windows → `10,631` alerts.
- **Macro Churn Finding:** `5.83 GB` gross churn across `11.59M` content edits (`+2.20 GB` Net Growth vs. **`3.51 GB` / `60.2%` Rewriting Volume**).

---

## Slide 8 — Major Real-World Events Automatically Surfaced (67 Days)

- **Nov 17–20:** *Riemann hypothesis*, *Alaska Senate election*, *GTA VI*, *Badr Jafar* (`195.4 KB` rewriting / `0 B` net across `32` reverts).
- **Nov 13–15:** *2026 FAA communication outage* (`87` edits / `28` editors in 1h, `z = +579.9`) + *FAA ground stop* & *2026 Microsoft outage* spillover.
- **Nov 07–09:** *2026 Kraków shooting* (`361` edits / `81` editors, `#1` dataset surge `z = +721.8`).
- **Nov 04–06:** *2026 US Midterm Elections* (*US House*, *US Senate* `29.72×` pageviews, *Keisha Lance Bottoms* edit war).
- **Nov 01–03:** *Palm Beach Learjet 60 crash* (`299` edits / `116` editors, caught `14`-edit candidate missed by static rules).

---

## Slide 9 — Contextual Graphs & Geospatial Intelligence (`Screens 04 & 05`)

- **2-Tier Subject Geospatial Map (`04_geospatial_map.png`):** Wikipedia coordinates + 3-hop Wikidata (`73.1%–83.1%` resolved, **`0` Editor IPs**), rendered via Plotly `Scattergeo`.
- **Spillover Graph, Diffs & Lift Wing (`05_context_graph_risk.png`):** `608` 1-hop edges (`57.4%` co-edited within `±1h`) rendered via Plotly `Scatter` network · `100%` usable Compare diffs · `75.4%` Spark pre-filter before Lift Wing (`τ = 0.65`, `76.92%` F1).

---

## Slide 10 — Anomaly Detection & Distributed Spark ML (`Screens 06 & 07`)

- **Detector B (Explainable 0–100):** 5 additive components + dampening guardrail (`65.04% → 9.14%` false alerts).
- **Detector C (`pyspark.ml`):** Stage 1 `VectorAssembler → StandardScaler → KMeans(k=6)` (`Silhouette 0.5546`) → Stage 2 `9D` `RandomForestClassifier(40 trees)` (`100%` precision on 8d eval, `817/817`).
- **Data Engineering for ML at Scale:** Zero Python UDFs (pure Catalyst/Tungsten `log1p` & `vector_to_array`), `km_scored.cache()` DAG reuse across stages, deterministic SHA-256 `alert_id`, and `75.4%` Spark pre-filter before Lift Wing.

---

## Slide 11 — Production Data Engineering & System Health (`Screen 08: /system`)

- Multi-member gzip recovery (`269,017` rows salvaged), CRC32 title fallback, atomic staged Parquet writes, zero-duplicate checkpointed outage drain (`123,537` events in `13.74s`), `< 1.35s` PyArrow serving SLA, and `45` automated tests.

---

## Slide 12 — Statistical Foundation: Historical Baselines

- **Sparse Zero-Inclusive Activity Profiling:** Accounts for every quiet hour across the 47-day baseline window in a single sparse Spark pass without materializing a `3.73B`-row dense grid, with a minimum variability floor for dormant articles.
- **Burst-Aware Editor Scaling & Conflict Separation:** Calibrates expected contributor counts to realistic collaborative editing sessions rather than uniform clock slices, and tracks constructive net growth separately from rewriting volume.
- **4-Tier Profile (`3.31M` Pages):** `92.31%` Quiet · `6.57%` Moderate · `1.04%` Active · `0.084%` Hyperactive.

---

## Slide 13 — Live System Demonstration

- 5-minute guided walkthrough across all 8 interactive Dash + Plotly screens (`http://127.0.0.1:8050`).

---

## Slide 14 — Key Takeaway: How WikiPulse Solved Wikipedia's Fixed-Threshold Problem

- **Problem:** Naive fixed thresholds suffer a `65.04%` false-alert rate on hyperactive list pages and edit wars while missing quiet-page surges and 2-editor rewrites.
- **Solution:** Per-page zero-inclusive baselines, session-aware editor scaling, rewriting-volume separation, `≥ 2.5×` pageview confirmation, and zero-UDF two-stage Spark ML — cutting false alerts from `65.04%` down to **`9.14%` (Explainable Score)** and **`0.00%` (Spark ML)**.

---

## Slide 15 — Thank You!

### Thank You!
WikiPulse — Real-Time Detection and Analysis of Wikipedia Knowledge Shifts
