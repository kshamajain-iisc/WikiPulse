# WikiPulse

WikiPulse studies unusual changes in Wikipedia editing activity: edit bursts,
reversions, editor participation, content churn, and bot activity.

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
imports pandas and matplotlib. There is no dependency manifest yet.
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

The repository is a collection and batch-analysis prototype. The planned Kafka
and Spark streaming pipeline, live dashboards, pageview joins, risk-model
integration, related-page graph, and geospatial views are not implemented here.
No automated test suite or dependency manifest is present. Burst and controversy
scores are exploratory heuristics, not validated detection models; the script
and notebook also use different minimum edit thresholds for controversy scoring.

Early sample-collection experiments and their local data remain in the sibling
`wikipulse-ideation` folder; they have not been merged into this codebase.
