# WikiPulse dashboards

A dedicated Dash + Plotly presentation package for the live, historical,
relationship/map, investigation, and engineering views in the
[final proposal](../docs/proposal/WikiPulse_Final_Proposal.pdf).

## Run locally

From the repository root:

```sh
./run-local.sh
```

Open [WikiPulse](http://127.0.0.1:8080). The launcher now starts
`python -m dashboards.app` instead of the original HTML viewer. It still manages
Kafka, topic creation, the collector, and both Spark measurement jobs.
Dependencies are pinned in the root `requirements.txt` and installed by the launcher.
Stop an already running older launcher before starting the updated one.

To serve dashboards alone against existing stored data:

```sh
.venv/bin/python -m dashboards.app --port 8080
```

The web server binds to loopback. Java 17 and the project virtual environment are
required. There is one Spark session in the dashboard process, shared by all pages.
No Node.js build, pandas, or separate frontend server is required. The bundled
Plotly JavaScript is served locally by Dash. A populated geographic base map may
retrieve Plotly's geographic topology over the network.

## Folder structure

```text
dashboards/
  app.py                 Dash factory, CLI, health and activity endpoints
  callbacks.py           Routing, shared filters, refresh, article navigation
  layout.py              Sidebar, application shell, common controls
  components.py          KPI cards, panels, safe tables, empty states
  charts.py              Plotly Graph Objects built from bounded Python lists
  data_service.py        Short server cache and serialized backend access
  pages/
    live.py              Live pulse, churn, activity ranking, alert feed
    investigation.py     Article timeline, baseline, revision/risk evidence
    historical.py        Daily history, source coverage, page baselines
    relationships.py     Article graph, subject map, provenance tables
    system.py            Raw quality, streaming progress, lag, storage, benchmarks
  assets/
    dashboard.css        Shared visual theme and responsive breakpoints
src/
  dashboard_queries.py   Spark queries and bounded JSON serialization
```

The earlier `src/activity_dashboard.py` remains as a compatibility utility and for
its existing tests; it is no longer the frontend launched by `run-local.sh`.

## Proposal coverage

| Screen / URL | Implemented presentation | Source requirements |
| --- | --- | --- |
| Live pulse `/` | KPI cards, human/bot trend, contributor share, top articles, byte churn, activity ledger, alert feed | Raw events or finalized windows; optional `alerts` |
| Page investigation `/investigation` | Per-article trends, baseline comparison, content changes, risk/reversion evidence | Activity, optional baselines, `revision_diffs`, `revert_risk_eval` |
| Historical intelligence `/historical` | Date filter, daily human/bot activity, rankings, byte movement, baseline profiles, last source date and observed-date coverage | Canonical `edits` or `rc_events`; optional baseline tables |
| Relationships & map `/relationships` | Bounded graph, article-subject coordinates, directional edge evidence, location source and quality | `graph_edges`, `map_points` |
| Engineering & health `/system` | Revalidated quality, actual Spark rates/batch duration, Kafka-to-Bronze backlog, Parquet footprint, numeric benchmark evidence | Raw events, runtime progress snapshots, optional benchmark reports |

No records are invented when a producer is absent. Alerts do not become confirmed
news from edit counts alone. Risk probabilities, pageview corroboration, and graph
propagation/causality are not inferred by the frontend. Missing location data never
invalidates an activity record. This implementation provides the presentation and
read path; later detector/enrichment/experiment producers still need to emit their
respective tables. Exact distinct-editor tracking across an entire horizon is not
claimed: the ledger shows the peak approximate human-editor count per window.

Week 3 now supplies the detector and alert tables. `run-local.sh` starts a detector
for each finalized duration. The feed shows up to 200 recent alerts for the selected
duration, independently of provisional activity previews. Article investigation
shows component points, confidence, available weight, and missing evidence.
See [the Week 3 runbook](../docs/week3.md) for score semantics and offline demos.

## Data paths and table contracts

Defaults:

- Raw: `RAW_PARQUET_PATH` from `.env` (normally `data/raw/wikipedia_edits`).
- Live: `data/live_activity/windows_5m` and `windows_1h`.
- History: `data/week2/historical/edits`, `baselines_5m`, `baselines_1h`.
- Curated: `data/curated`. Week 3 alerts use the runtime contract in
  `src/detection/scoring.py`, documented in `docs/week3.md`; other optional tables
  use the milestone schemas in `src/models/schemas.py`.
- Runtime: `data/run/*.progress.json` written by the updated Spark jobs.

Alternate table roots can be selected explicitly:

```sh
.venv/bin/python -m dashboards.app \
  --live /path/to/live \
  --history /path/to/historical \
  --curated /path/to/curated
```

The canonical `rc_events` history table must contain the normalized edit schema.
The historical screen does not read unverified raw gzip data directly. Prepare a
complete collection first with the commands in [the pipeline runbook](../docs/week2.md).
Historical date filters are inclusive; the collection/preparation end bound remains
exclusive. Historical charts list observed dates and do not fabricate missing days.
The normalized history must retain the collector's completion evidence separately:
observing an event on a date does not by itself establish full-day completeness.

Optional tables use the existing model schemas: `alerts`, `graph_edges`,
`map_points`, `revision_diffs`, `revert_risk_eval`, and `page_baselines`.
The engineering screen also reads flat numeric JSON reports from
`<curated>/benchmarks/*.json`, retaining units in the measurement names.

## Refresh, limits, and semantics

- All dataframe processing happens in Spark. Plotly uses `graph_objects` with
  ordinary lists; there are no pandas imports, pandas UDFs, or `toPandas()` calls.
- Live preview is provisional. It deduplicates stable event IDs and shows the last
  30 event-time minutes (five-minute mode) or two hours (hourly mode), anchored to
  the latest observed source event. The source timestamp is always displayed.
- Finalized windows retain the stream's watermark delay and show a recent two-hour
  horizon. A closed window and a provisional current window are distinct concepts.
- Metrics aggregate the full selected scope before limiting rankings to 100 rows.
  Charts use at most 300 timeline points, 10 ranked bars, and 200 enrichment records.
  Graph layout shows at most 60 nodes; edge tables preserve original direction.
- Current/prior ingestion-date partitions bound live scans. Typed Parquet event-time
  metadata narrows the horizon before raw JSON is parsed. Full-day quality checks
  on the system page can be more expensive than the live horizon query.
- Automatic refresh runs every 30 seconds, with a pause checkbox. Snapshots are
  cached for 20 seconds; a server lock serializes Spark work across browser tabs.
  An initial uncached query may take several seconds. This is not a measured
  guarantee of the proposal's sub-two-second stored-result query target.
- Backlog is measured against the last committed Bronze Kafka offsets. It is not
  window-state delay. Unavailable broker metrics are displayed as unknown.
- Spark progress snapshots are emitted by the updated supervised jobs after a
  completed batch. Old/stale progress is labeled, rather than presented as healthy.
- Article links and chart clicks open an investigation URL with the selected title.
  External revision text is rendered as text, never executable HTML.

## Validation

```sh
.venv/bin/python -m unittest discover -s tests -v
```

`test_dashboards.py` checks all five page renderers, real Dash callback HTTP payloads,
article routing, empty figures, and cache reuse. `test_dashboard_queries.py` runs
Spark against duplicate events without numeric page IDs and verifies full-scope
KPIs, article filtering, missing sources, and current validation semantics. Existing
streaming and normalization tests remain part of the suite.

Verification performed: the complete 25-test regression suite passed. After the
final article-navigation change, all eight focused Dash presentation/HTTP tests
also passed, including a single-URL article-picker callback and optional graph
input on non-live routes. The Spark dashboard query test passed with duplicate
input and no numeric page IDs. Dependency, compilation, and whitespace checks
passed. Browser checks on the running stack confirmed real live charts, article
investigation, historical/graph empty states, measured Spark progress, and broker
backlog. The local launcher is serving this Dash package at port 8080.

The final browser check also selected a real article through the picker and verified
its encoded investigation URL, article heading, and charts, with no browser console
errors. The existing in-app browser tab was refreshed to the new frontend and left
on the live overview.

Chart sizing correction: graph containers now reserve the figure's explicit pixel
height instead of inheriting Dash's default percentage height from an auto-sized
card. Grid columns can shrink horizontally, while graph heights cannot collapse.
Nine focused dashboard tests passed. Browser measurements confirmed 300-pixel
investigation plots fully contained in their cards, including the empty baseline
plot, and matching SVG/container widths after desktop/narrow viewport changes.
