# WikiPulse

WikiPulse studies unusual changes in Wikipedia editing activity: edit bursts,
reversions, editor participation, content churn, and bot activity.

## Run the local stack

Start your Docker-compatible container engine, configure a real
`WIKIMEDIA_USER_AGENT` contact in `.env`, then run:

```sh
./run-local.sh
```

This bootstraps Python dependencies, starts Kafka, creates the topic, and launches
the collector, Spark raw ingestion, five-minute/hourly measurements, and the
[Dash + Plotly dashboards](http://127.0.0.1:8080). Choose either duration in the dashboard. The default live preview shows provisional
counts immediately; select finalized windows for watermark-complete results.
Use Java 17 and Python 3.12. Spark runs locally; no separate Spark cluster is needed.

Press Ctrl+C to stop the stack. Saved data and checkpoints are retained; a Kafka
broker that was already running is left running. Service logs are in `logs/local/`.
Use `./run-local.sh --check` for prerequisite checks or `--port 8081` for another port.
See [the local runbook](docs/week2.md#one-command-local-startup) for first-run timing
and historical baseline setup.

## Dashboards

The frontend lives in the separate [`dashboards/`](dashboards/README.md) package,
with five connected screens based on the proposal. Spark queries remain in `src/`.
The local launcher serves the Dash application at port 8080.

## Week 2

Spark historical baselines, checkpointed streaming page windows, and a local live
activity viewer are implemented. See [Week 2 architecture and runbook](docs/week2.md)
for setup, commands, feature definitions, and validation. Week 2 uses no pandas.

## Week 3

Explainable knowledge-shift detection, a Kafka alert topic, historical replay,
controlled test patterns, and an initial Spark ML comparison are implemented.
The local launcher starts both finalized-window detectors, and the dashboard
shows alert reasons and component points. See [Week 3 implementation and architecture](docs/week3.md)
for the file inventory, score definitions, recovery guarantees, and commands.

Run a repeatable offline experiment without Kafka or Wikimedia access:

```sh
.venv/bin/python -m src.replay.demo --output data/experiments/week3-demo --with-ml
```

## Implemented so far

- `enwiki_collect_recentchanges.py`: English Wikipedia recent-change collection
  for configurable UTC windows, with daily compressed JSONL files, checkpoints,
  run logs, retry/backoff handling, and a disk cap.
- `enwiki_collect_revisions.py`: revision collection through the allrevisions API
  with the same daily storage and resumable collection approach.
- `spark_analysis.py`: batch analysis of event volume, hourly human/bot activity,
  hot pages, revert-based controversy candidates, hourly burst candidates,
  editor activity, edit-size percentiles, and tags; exports hot pages to Parquet.
- `spark_analysis.ipynb`: visual analysis with saved charts and execution output.
  Its saved output reports 293,627 changes for September 16, 2026 and a successful
  export to `data/curated/hot_pages_2026-09-16`. This is prior execution evidence;
  the underlying dataset and Parquet output are absent from this checkout.

## Running the existing code

The collectors use Python's standard library. Set a real contact address in their
`USER_AGENT` constants and choose explicit dates before collecting:

```sh
python3 enwiki_collect_recentchanges.py --help
python3 enwiki_collect_revisions.py --help
```

Spark analysis requires PySpark and a compatible Java runtime. The notebook also
imports pandas and matplotlib. Dependencies are pinned in `requirements.txt`.
Run analysis from the repository root with the expected source file available:

```sh
python3 spark_analysis.py
```

Both analysis files currently read
`data/enwiki_recentchanges/rc_2026-09-16.jsonl.gz`.
The script writes `data/curated/hot_pages`; the notebook writes the date-specific
path noted above. Both overwrite their target output. Collector defaults use
September 15, so they do not produce the analysis input without explicit dates.

## Project documentation

The following documents were moved from `wikipulse-ideation` on October 27, 2026.
Their existing internal folder structure and relative links were preserved.

- [Final project proposal](docs/proposal/WikiPulse_Final_Proposal.pdf)
- [Project concept and knowledge shifts](docs/ideas/00-wikipulse-knowledge-shifts.md)
- [Simple architecture](docs/simple-architecture.md)
- [Course feature catalogue](docs/course-feature-catalog.md)
- [Wikipedia API reference](docs/wikipedia-api-reference.md)
- [Postman collection](docs/postman/wikipulse-wikimedia-apis.postman_collection.json)
- Feature plans: [unusual activity radar](docs/ideas/01-breaking-news-radar.md),
  [edit risk](docs/ideas/02-edit-risk-monitor.md),
  [geospatial map](docs/ideas/03-global-geospatial-map.md), and
  [churn and bot ratio](docs/ideas/04-knowledge-churn-and-bot-ratio.md).

## Remaining work

The repository includes collection, batch analysis, streaming measurements,
explainable detection, replay, offline ML comparison, and five dashboard screens.
Pageview joins, revision-risk model integration, related-page graph production,
and geographic enrichment remain future work. The frontend supports their
optional tables. Week 2 integration tests are in `tests/test_page_measurements.py`;
Week 3 tests are in `tests/test_detection.py` and `tests/test_replay.py`. Burst and controversy
scores are exploratory heuristics, not validated detection models; the script
and notebook also use different minimum edit thresholds for controversy scoring.

Early sample-collection experiments and their local data remain in the sibling
`wikipulse-ideation` folder; they have not been merged into this codebase.
