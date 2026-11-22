# WikiPulse — Real-Time Detection and Analysis of Wikipedia Knowledge Shifts

**Team QuadCore:** Kshama Jain, Senthil Raj K, Pothukanuri Sai Venkat, Akash H S  
**Course:** DA231o — Data Engineering at Scale, Indian Institute of Science (IISc), Bengaluru

WikiPulse monitors live and historical English Wikipedia edits to detect real-world events, separate breaking news from edit wars and major rewrites, and explore article activity through an 8-screen interactive dashboard.

---

## Tech Stack

- **Streaming:** Wikimedia EventStreams (SSE) & Apache Kafka (KRaft)
- **Processing & ML:** Apache Spark (`pyspark.sql`, Structured Streaming, `pyspark.ml`)
- **Storage:** Partitioned Snappy Parquet & PyArrow
- **Visualization:** Dash & Plotly (`graph_objects` for charts, spillover networks, and geospatial maps)

---

## Project Structure

- `src/` — Streaming collector, Kafka replay, Spark batch/streaming jobs, anomaly scoring, and enrichment pipelines
- `dashboards/` — 8-screen Dash + Plotly web application
- `tests/` — Automated unit and integration tests
- `Documentation/` — Final report (PDF/HTML), presentation deck (PPTX/HTML/MD), and guides
- `docs/` — Setup prerequisites and runbooks

---

## Quickstart

**1. Run the live stack (Kafka + Collector + Spark + Dashboard):**
```bash
./run-local.sh --port 8050
```

**2. Run the offline backup demo (from curated Parquet data):**
```bash
.venv/bin/python3 Repo/src/demo/backup_demo.py --port 8050
```

**3. Run tests:**
```bash
PYTHONPATH=Repo .venv/bin/python3 -m unittest discover -s Repo/tests -v
```

---

## Documentation

- [Project Report (PDF)](Documentation/WikiPulse_Project_Report.pdf)
- [Final Presentation (PPTX)](Documentation/WikiPulse_Final_Presentation.pptx) · [Interactive HTML](Documentation/WikiPulse_Final_Presentation.html)
- [User Guide](Documentation/User_Guide.md) · [Technical Documentation](Documentation/Technical_Documentation.md) · [Data Setup](DATA_SETUP.md)
