# Implementation verification

## Delivered scope

- LP01: a docked processing console on the two activity routes, with measured rows,
  decoded bytes, source footprint, cache use, queue/query timing, stage output and logs.
- LP02 and LP04: bounded asynchronous serving, early core results, optional evidence
  isolation, stale-selection skeletons, request cancellation and retry.
- LP03: article and Back to Live Pulse links preserve the source, day, timezone,
  horizon and playback position; Plotly UI revision preserves same-scope interaction.
- LP05 and LP06: Live/Stored modes, observed dates, half-open IST/UTC day bounds,
  complete scoped totals and article filtering before display limits.
- LP07: elapsed-time saved playback, supported speeds through 100×, seek, restart,
  pause, cancellation, first-capture shortcut and cumulative versus rolling counts.
- LP08: authenticated resident-controller start/stop, intake flush, offset/file drain
  checks, pending-window status and a dashboard that remains available when stopped.
- LP09: optional isolated Kafka and Spark replay with publication pause/cancel,
  run-specific topics/output/checkpoints and reconciliation counters.

## Automated checks

The focused suite passed 40 tests:

```sh
.venv/bin/python -m unittest tests.test_activity_workspace tests.test_dashboards tests.test_run_local tests.test_dashboard_routes -q
```

Python compilation and `git diff --check` passed. Existing unrelated edits to
`.env.example`, `src/page_features.py` and `src/stream_measurements.py` were retained.

## Real pipeline and browser checks

An isolated run of three explicitly synthetic events used a separate Kafka topic
and temporary Spark checkpoint/output. Selected = published = consumed = committed
= 3; rejected = pending = 0. The new board reader returned three edits, one article
and 33 bytes of content movement from that committed output.

Browser checks on port 8080 confirmed populated Live Pulse and article activity,
date-selected saved replay, 50× clock advancement, pause, and an article link carrying
the selected date and cursor. The investigated article remained fixed. New requests
show skeletons rather than the previous page's charts. The live/stored console showed
actual row and decoded-payload measurements. No client errors were observed after
the final reload during these checks.

ML Tests and Reports retained its existing presentation, and the new workspace and
console were hidden on that route. No navigation restructuring or other page
enhancements were introduced.

The first actual stop probe honestly reported unverified Kafka drain. Its local
address-family handling was corrected to match the broker's IPv4 binding; the
read-only target-offset probe then succeeded. The final UI stop returned
`drained: true`, `warnings: []` and left the dashboard available. Start then resumed
all seven services from compatible checkpoints. Fresh events arrived, and Live Pulse
showed 245 edits across 226 articles in the observed five-minute scope. The console
heading stays visible while scrolling the drawer, including on narrow screens.

## Measurement limits

The development timings in README are individual observations rather than a p95 or
overhead benchmark. Actual disk I/O remains explicitly uninstrumented. Date labels
do not claim full-day capture. Large-day memory use depends on that day's selected
working set, and a completed three-event integration run does not establish full-day
100× processing capacity.

## Follow-up QA and UI revision — 16 Nov 2026

The saved-date selector is now a calendar with previous/next captured-date buttons.
The common filters share one grid: one live row or two stored rows at desktop widths,
with wrapping on smaller screens. The console occupies a right results column on
wide screens and a collapsible right drawer on smaller screens. Its scroll heading
stays visible. Ledger links have a block hit target and a readable minimum width.

Additional defects found by one-by-one testing and corrected:

- Snapshot hits now report zero fresh scan/analysis work while retaining matching
  result counts; day-index hits report zero fetched rows/decoded bytes. Finalized
  mode distinguishes aggregate window rows from unique event analysis.
- Result JSON size is labelled accurately; it excludes Dash chart markup and HTTP
  overhead. Source logs name only the actual selected sources. Kafka results are
  labelled as Spark-committed replay output.
- Cold article reads no longer narrow the shared search catalogue to one article.
  A projected title search can discover other captured articles.
- Finalized investigation without a selected article returns an explicit selection
  prompt with zero totals, rather than totals for all articles.
- Unicode article identities now use the same JSON encoding as browser scope keys;
  titles such as Einstein–Cartan theory render instead of remaining behind a loader.
- A paused Kafka publisher continues polling delivery acknowledgements.
- Idle Spark progress heartbeats do not consume the batch ID used by the next real
  data batch. The long-pause test exposed an undercount of 131 events even though all
  2,694 acknowledged events were present in committed Parquet. That test run was
  retained with its honest failure status; a regression covers heartbeat reuse and
  the corrected pipeline is verified again below.

Browser checks already covered all live horizons, freeze across refresh intervals,
resume, finalized totals, calendar clicking and typed dates (Enter applies typing),
IST/UTC summaries, missing dates, replay seek/first event/restart/speeds/pause/cancel,
fixed investigation titles, chart and keyboard ledger navigation, captured-article
search, Wikipedia links, preserved Back-link context and the untouched ML Reports
route. The actual request-log export returned attachment headers, and its stored
article matching count reconciled to three edits. A 100× clock advanced about 961
seconds in 9.65 wall seconds and then remained fixed after Pause.

The initial runtime controller was absent while orphaned workers remained alive;
the command correctly refused to claim a successful stop. Restarting the managed
launcher restored controller ownership. The actual Stop then drained successfully
(`drained: true`, no warnings), keeping the dashboard available. Start was exercised
through the UI and its final receipt is checked below.

### Final follow-up results

The final focused suite passed **66 tests** (the original suites plus activity,
replay, callback and telemetry regressions). Compilation and whitespace checks
passed. Browser errors from intentional server restarts were excluded only from
the post-reload check; no client errors occurred after the final reload.

| Feature | Verification | Result |
| --- | --- | --- |
| Live 5m / 1h / 6h | Real populated browser views and exact fixture boundaries | Pass |
| Freeze / resume | Job ID and snapshot time held across refreshes while capture continued; unfreeze refreshed | Pass |
| Finalized windows | Real populated view; row/event distinction; article required in fixtures | Pass |
| Calendar / captured-date arrows | Calendar click, typed date + Enter, 27↔28 Oct stepping | Pass |
| IST / UTC summaries | Real date-bound totals; exact half-open fixture bounds | Pass |
| Missing dates / empty selection | Honest empty scope and selection prompt; no silent date/article replacement | Pass |
| Saved replay | Seek, first event, restart, pause, cancel; deterministic tests for every offered speed | Pass |
| Replay timing | About 961 event seconds in 9.65 wall seconds at 100×; pause held position | Pass |
| Investigation | Live and stored results, fixed article during playback, Unicode title, Wikipedia URL, clear selection | Pass |
| Navigation / loading | Context-preserving chart/ledger/back links, hidden stale charts, independent optional evidence | Pass |
| Article discovery | Captured-scope search including titles outside top 100; cold article regression | Pass |
| Console accuracy | Zero fresh work on snapshot hits; day-index work distinguished; exact export matching count; accurate source/byte labels | Pass |
| Console behavior | Manual collapse retained, right desktop column, sticky header after scrolling, right mobile drawer | Pass |
| Layout | 1 Live row / 2 Stored rows at 1440 CSS px; also 2 Stored rows at 1108 px; no settled horizontal overflow at 390 px | Pass |
| Mobile calendar | Popover fit within 390 px viewport (left 15, right 326); console header stayed at 17 px | Pass |
| Stream Stop / Start | Actual UI stop returned drained=true, warnings=[]; UI start resumed all 7 workers | Pass |
| Kafka start / pause / resume | Real UI run, isolated topic/output; ~57 second pause drained to 2,402 acknowledged/consumed/committed events | Pass |
| Kafka detach / cancel | Saved controls re-enabled; cancellation drained to 4,734 published=consumed=committed, zero pending/rejected | Pass |
| Kafka output reconciliation | All 4,734 rows independently counted from committed Parquet manifests | Pass |
| Kafka completion | Separate labelled three-event run completed with all counts=3, zero pending/rejected | Pass |
| Failure / retry / cancellation | HTTP callback and worker fixtures cover query failure, retry, superseded requests and optional evidence | Pass |
| Other pages | ML Reports retained its renderer; new workspace/console hidden; legacy route suites passed | Pass |

The three-event completion output is retained at
`/private/tmp/wikipulse-replay-verification/finalqa20261116`.
The successful pause/resume/cancel UI run is retained at
`data/replays/851f149569eb4d70ad5adb95d9c9828b`.
Its selected source had 119,546 edits; cancellation intentionally processed only
4,734, so this is not a completed full-day throughput benchmark.

A final loading check also caught status updates from a detached Kafka run creating
unnecessary saved-summary refreshes. Status-only background updates now leave those
snapshots alone; detachment refreshes the saved source once, and terminal runs stop
polling their manager. Three HTTP callback regressions verify these boundaries.

The original unverified capacity limits remain: no p95 or full-day 100× capacity
claim is made. Managed stream actions require the `./run-local.sh --port 8080`
launcher to remain running. The final stack and browser state are checked after
reloading this last polling correction.

Final handoff validation of the last revision: the actual running Dash endpoint
returned HTTP 204 for a detached Kafka status update, all seven managed services
were running, and the browser retained exactly the same completed saved-summary
job and timestamp for 62.9 seconds. No client errors were observed after that final
reload. The selected UTC day contained 119,546 edits across 83,046 articles; its cold
read measured 9.79 MB of decoded Arrow payload from a 23.23 MB selected file footprint.
Raw disk I/O was not measured. See `qa-browser-observations.json` and
`qa-runtime-observations.json` for the captured evidence.

## Configurable chart refresh — 16 Nov 2026

Added a top-toolbar **Chart refresh** selector: 10 seconds, 30 seconds (default), or
1 minute. Its local preference persists between the two activity pages and reloads.
The one-second replay clock remains smooth; chart frame scheduling uses elapsed wall
time at the selected cadence for Live, saved replay and attached Kafka replay.
Manual scope changes, seek, restart, Play/Pause and Refresh / Retry remain immediate.
Day summaries, paused saved replay and completed Kafka runs do not refresh on a timer.
Changing cadence resets the wait without moving playback position or forcing a query.

Job progress now polls only while pending. Kafka status polls independently every
two seconds while active; those counters no longer trigger a chart query per update.
A terminal Kafka transition still requests the final committed frame. Scheduled work
does not cancel a query already running. Cached identical core/evidence components
are retained, and same-scope evidence stays visible while refreshed evidence loads.
The shared loading pill is hidden only on the activity routes to avoid header flashes.

The focused suites passed 73 tests, including deterministic clock cases for every
refresh option, unchanged 100× playback timing, dynamic interval changes, paused and
terminal replay, Kafka status-versus-chart scheduling, slow-query backpressure,
manual cursor accuracy, retained evidence and cached chart reuse. Browser checks
confirmed the selector, persisted one-minute selection after reload, no query from
changing cadence on a day summary, and no live query during the first 12.2 seconds
of a newly selected 30-second interval.

Final refresh UI validation: paused replay held the same clock and job for 56.8
seconds; changing cadence did not move that position. A 100× replay advanced about
4,795 event seconds over a 48.3-second observation (with normal sampling/command
latency), independently of 10-second chart updates. The 30-second preference was
preserved in Page Investigation, and the desktop Live toolbar still fit one row at
1440 CSS px without overflow. No browser errors appeared during the cadence checks.
The final selected preference is 30 seconds, Live data is populated, and all seven
managed services are running. Evidence is in `qa-refresh-observations.json`.

## Stable browser title and two-button replay — 16 Nov 2026

Dash's `update_title` configuration was still changing the tab title for background
requests. It is now disabled; the title remains WikiPulse while in-page progress
continues. The served configuration is regression-tested.

Replay now exposes **Start replay**, **Stop replay**, and a closed **Replay settings**
disclosure. Removed separate Restart day, First captured event, Cancel replay, Kafka
Start/Pause/Cancel and Return-to-saved action buttons. Settings holds processing
(Saved playback / Kafka & Spark demo), speed and seeking. Both engines use the same
two buttons. Saved Start automatically skips to the first matching event or resumes
its stopped position; Start after completion begins again. Stop requests a final
saved snapshot. Kafka Stop drains admitted messages and retains partial results;
a subsequent Kafka Start begins a new isolated run. Changing processing requires
stopping first, and changing source/date/timezone/view stops an old active run.

Tests passed: **80**. Added title configuration, two-action layout, saved Start
without Kafka, shared Kafka Start/Stop, scope-change cancellation, idempotent
Start/Stop and warm-index article-first-capture regressions. Existing refresh,
source-boundary and route suites also passed.

Browser checks confirmed automatic positioning at 00:00:01 UTC, active Start/Stop
button states, a saved Stop at 00:31:05 and resume at the retained position. The same
Start button launched an isolated Kafka run; the same Stop button drained it to
**1,342 published = consumed = committed = actual Parquet rows**, zero pending and
zero rejected. Processing was locked while active and unlocked after stopping.
The saved-data option remained available in settings afterward. The browser title
was unchanged at initial load, saved Start/Stop, Kafka Start, background processing,
Kafka Stop and return to saved playback, spanning over three minutes.

Final layout verification: only Start replay and Stop replay are visible as replay
action buttons with settings closed, at both 1440 px desktop and 390 px mobile
widths. Neither viewport overflowed horizontally. All sampled titles were identical,
and no client errors occurred during the new replay checks. Details and title
samples are in `qa-simple-replay-observations.json`. The refreshed app was left in
Saved playback with settings closed and live capture still running.

## Persistent, readable processing console — 16 November 2026

Implemented the authorized console follow-up on Live Pulse and Page Investigation.
`activity_console.py` provides a thread-safe 500-entry view backed by rotating JSONL
files under `data/run/activity-console/` (three 1 MB files). Entries restore across
workspace/server initialization; malformed partial final lines do not discard valid
history. Write failures preserve the in-memory log and display a retention notice.
No database or new dependency was added. Every refresh receives a correlation ID,
a start entry and exactly one measured terminal summary, including cached, failed
and cancelled queries. The UI now renders retained history rather than the active
job's entries, and Download history serves the retained JSONL attachment. Per-job
JSON downloads remain available.

Removed the five internal stage cards. Readable capture messages identify the saved
files used by dashboard queries, distinguish stored file footprint from decoded
payload, and report actual rows, unique events and processing duration. The latest
Spark ingestion batch includes count/duration/timestamp and is explicitly sampled.
A separate one-thread observer checks the real Kafka broker without consuming,
committing offsets or creating topics. Retention is a labelled approximate low/high
offset span, never invented stored bytes. Unknown broker measurements stay unknown.
The probe's completion timestamp is retained, so stale samples cannot appear fresh.

Validation: the six new console tests plus the existing 80 tests passed (86 total,
23.256 seconds). Additional cancellation/failure history assertions were then run
with the console tests (10 passed). Tests cover append-only history, cache-zero-work
summaries, disk restore, retention/rotation, interrupted final writes, memory fallback,
HTTP attachments, worker nonblocking behavior and read-only Kafka offset inspection.
Browser verification confirmed previous history remained after automatic and manual
refreshes, a move to an article investigation, and a browser reload. The 500-entry
visible cap was observed. Both narrow drawer and desktop right-column layouts were
checked, with no desktop horizontal overflow. The browser title remained stable.

A real saved-data read analyzed 119,546 events and decoded 9.79 MB in 1.53 seconds;
this is one development observation, not a benchmark. A real Kafka metadata sample
reported three capture-topic partitions and approximately 21,816 retained record
slots; subsequent samples increased. The HTTP history download returned 200 with
an attachment filename, 103,560 bytes, 340 valid JSONL entries and 32 refresh
summaries. The in-app browser's download event was not surfaced to automation;
the attachment body/headers and link target were independently verified. All seven
managed services were running after verification. Raw QA observations are in
`qa-console-history-observations.json`.

### Collapsed by default

Removed slow-request/failure auto-expansion and session-persisted open state. The
console initializes closed on every page load and opens only through its heading;
refreshes do not alter that choice. Its log collection and local history continue
while closed. All 60 activity workspace/console tests passed. Browser checks verified
closed during loading, closed after completion, manual opening, and a fresh closed
state after reloading an opened console.
