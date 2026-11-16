# Live Pulse and Page Investigation enhancements

Only `/` and `/investigation` use the new activity workspace. Existing navigation
entries, other page renderers and their query/cache paths remain intact.

## Using the two screens

- **Live** reads committed Bronze edits in the chosen 5-minute, 1-hour or 6-hour
  horizon. Finalized windows are an explicit watermark-delayed alternative.
- **Freeze display** stops automatic view updates; ingestion continues. Clearing it
  requests a new snapshot. The snapshot timestamp remains visible.
- **Chart refresh** at the top offers 10 seconds, 30 seconds (default) or 1 minute.
  The preference persists across these two screens and browser reloads. It controls
  Live, saved-replay and attached Kafka-replay chart updates. The replay clock still
  advances each second at the chosen playback speed. Seeking, selecting a scope,
  stopping and manual Refresh / Retry apply immediately. Day summaries and stopped
  saved replay stay idle. A slow query finishes before another scheduled one starts.
- **Stored activity / Day summary** aggregates all matching mainspace edits before
  limiting the displayed ranking. Use the calendar or the arrows to step between
  captured dates, and choose IST or UTC day bounds. The common filters occupy one
  row for Live and two for Stored at desktop widths; smaller screens wrap controls.
  Explicit unavailable dates in links are retained and show an empty result.
- **Stored activity / Replay** has two action buttons: **Start replay** and
  **Stop replay**. Start automatically skips an uncaptured start of day and resumes
  a stopped position. At the end of a day, Start begins again at the first captured
  event. Stop retains the position and loads its final snapshot. Both rolling edits
  and the selected scope's day total so far are displayed. Optional **Replay settings**
  contains playback speed, seeking, and the choice of Saved playback or Kafka & Spark.
- Article and chart links preserve source, date, timezone, horizon and replay position.
  Back to Live Pulse carries that context back. Article search uses the captured
  scope, including titles beyond the displayed top 100.

## Loading and measurements

The right-side Processing Console exists only on these two routes. On wider screens
it reserves a column beside the charts; on narrow screens it becomes a right drawer.
Its heading remains visible when scrolling, and its collapsed tab stays on the right.
It starts collapsed on every page load, opens only when clicked, and can follow
the latest log entries. Each refresh appends to the history instead of replacing the
previous request. The console shows the latest 500 events shared across Live Pulse
and Page Investigation. Plain JSONL files in `data/run/activity-console/` retain up
to three 1 MB files, restore history after a server restart, and rotate automatically.
No database is used. Download history exports retained files; per-request diagnostic
downloads remain available at `/api/activity/jobs/<id>/log`. If local file writes
fail, the console reports memory-only retention and queries continue normally.

Jobs run on one bounded serving worker. Core metrics/charts become available before
optional baselines, alerts, revision differences and risk evidence. New selections
hide stale charts behind skeletons; obsolete jobs cancel between record batches.
Background refreshes retain existing evidence while new evidence loads, and exact
cached snapshots do not replace chart components. Job progress polls only while a
query is pending. Kafka progress polls separately while its run is active, without
refreshing charts on every status update. The global loading pill is suppressed on
these two screens so background requests do not flash the header each second.
Dash's automatic request title is disabled: the browser tab keeps its WikiPulse title
while health, query and Kafka requests run in the background.

The latest-refresh badges show filtered rows fetched, unique events examined, decoded
event payload and processing time. Every completed/cancelled/failed refresh writes a
summary to history. JSONL metrics also preserve matching edits, selected-file footprint,
queue duration and result JSON size when measured. Selected-file footprint is stored
compressed size, not disk reads. Arrow payload is materialized data, not network traffic.
Actual disk/network I/O is not instrumented. Snapshot cache hits report zero fetched rows, zero decoded bytes
and zero freshly analyzed events, while preserving matching edits from the result.
Day-index hits report zero rereads but still count events examined for aggregation.
Finalized mode reports page-window rows and labels individual-event analysis as
unavailable. Counts are scoped to the named operation; corpus size is never
presented as work performed on every refresh.

The internal five-stage cards have been replaced with readable capture status and
the latest reported Spark ingestion batch's input count, duration and timestamp.
That sampled batch is not a per-refresh ingestion total. A separate bounded worker
checks Kafka metadata and low/high partition offsets about every 30 seconds while
these pages are active. Its approximate retained record count is the sum of offset
spans, which may include gaps; Kafka stored bytes are explicitly unknown. No messages
are consumed, offsets committed or topics created by this observer. The dashboard
reads committed event files rather than querying Kafka directly. A failed broker
probe reports unknown values without blocking the serving worker.

The serving worker uses the existing PyArrow dependency with timestamp/namespace/
change-type predicates, article pushdown on cold article reads, projected columns,
and batches. Kafka and Spark continue producing the live lake. Spark file-sink
manifests determine which output files may be read. Stable event IDs deduplicate
overlapping canonical and Bronze data. Snapshots and up to two selected-day indexes
are retained independently; indexes above 250,000 events are not cached. The
temporary selected-day working set still depends on that day's event volume.

## Stream actions

Start/Stop stream is available in a managed `./run-local.sh` session. Stop flushes
the collector, verifies Kafka-to-Bronze offsets, drains committed file inputs in
dependency order, and stops stream workers while keeping the dashboard, broker and
controller alive. Drain verification has a bounded wait and reports pending work
instead of claiming success. Open event-time windows remain in checkpoints.

Start resumes compatible state. External EventStreams capture may have a gap while
stopped; the UI states that resumption does not establish complete capture. Lifecycle
commands use the existing control token, serialized request files and command IDs.
The ordinary global Restart stream control remains compatible.

## Isolated Kafka replay

Expand **Replay settings**, choose **Kafka & Spark demo**, and use **Start replay**. This
creates `data/replays/<run-id>` and topic `wiki.replay.<run-id>`. An independent Spark
query validates, deduplicates and commits replay output to that run's Bronze path.
The two boards switch to those committed outputs; the live topic and checkpoints
are never used for replay. Selected, published, consumed, committed, rejected and
pending counters are separate. **Stop replay** stops publication and drains admitted
messages, retaining partial results. While running, processing and speed settings are
locked; Kafka seeking is unavailable. After stopping, choose Saved playback in the
same settings to return to the saved-data view. Starting Kafka again creates a new
isolated run; resuming a retained position applies to Saved playback. There are no
separate Kafka buttons.
Changing the selected date, timezone, source or replay view stops an active old run.

At most one Kafka run executes at a time. Original event times/IDs are preserved.
The requested speed is fixed at launch, and startup/processing can lag publication.
Event gaps between the first and last captured events are paced; uncaptured day
edges are not artificially padded. Runs survive page navigation. Dashboard shutdown
cancels its owned replay workers. Output, status and checkpoint artifacts remain
available for inspection; automatic cross-process publisher recovery is not claimed.

## Verification

Run the focused suite:

```sh
.venv/bin/python -m unittest tests.test_activity_workspace tests.test_dashboards tests.test_run_local tests.test_dashboard_routes tests.test_replay -v
```

Tests cover half-open IST/UTC dates, duplicate events, complete totals before limits,
article pushdown, committed-file reads, replay rolling/cumulative counts, cache
invalidation, bounded request cancellation, progressive evidence, real Dash callback
payloads, control authentication/idempotency, checkpoint drain ordering and the
production JavaScript clock.

The existing launcher tests were isolated from real listener processes and aligned
with the launcher's existing broker-restart contract. The file-backed producer's
compatibility check no longer assumes the current Docker launcher has an absent
fallback branch.

An actual isolated Kafka/Spark verification selected, published, consumed and
committed three labelled synthetic events with zero rejects/pending events. Reading
its committed output through the activity reader returned those three edits and
33 bytes of content movement. This is an integration check, not a capacity benchmark.

Single direct local reads during development returned a populated live snapshot in
about 1.5 seconds and a 119,546-edit UTC day summary in about 0.7 seconds. These are
individual observations, not p95 guarantees or a telemetry-overhead benchmark.

The follow-up UI and one-by-one feature results, including the long-pause Kafka
reconciliation and 66 passing tests, are recorded in [implementation-log.md](implementation-log.md).
