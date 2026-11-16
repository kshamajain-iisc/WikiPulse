# Live Pulse and Page Investigation research

The approved Page is `WikiPulse Live Pulse and Page Investigation Enhancement Plan`.
Only `/` and `/investigation` gain new presentation and controls. Existing local edits
in `.env.example`, `src/page_features.py` and `src/stream_measurements.py` are preserved.

## Current boundaries

- `dashboards/callbacks.py:123` performs full synchronous page rendering. Other routes
  depend on this callback and must retain their current dispatch and filters.
- `dashboards/data_service.py:43` checks its cache under a Spark serialization lock.
  A separate scoped serving boundary can avoid this lock without allowing concurrent
  Spark queries or changing the other pages' cache policies.
- `src/dashboard_queries.py:379` reads typed Bronze event-time columns for live data.
  Canonical `rc_events` has `ts`, `rcid`, `pageid`, `ns`, bot flags, sizes and tags.
  Date partitions and Parquet predicate pushdown allow bounded selected-day reads.
- Existing PyArrow readers establish a dependency-free serving option. Use projected,
  filtered record batches and bounded retained day indexes for the two new views.
  Kafka and Spark remain the ingestion and processing pipeline; UI serving metrics
  must identify Arrow reads and must not claim another Spark computation occurred.
- `src/local_supervisor.py:73` authenticates restart requests to the resident supervisor.
  `src/run_local.py:578` handles those commands while retaining worker checkpoints.
  Start and stop need a resident stopped state and staged draining, not app shutdown.
- The current replay clock advances one horizon per 15-second tick. Replace it with
  an elapsed-time browser clock and independent bounded rendering requests.
- Article table links carry only title. Rewrite links only inside the scoped views
  so date, mode, horizon and cursor survive drill-down and return.

## Data flow

    controls -> scoped request -> bounded worker -> filtered Parquet batches
                       |                 |                 |
                 progress polling <-----+          activity snapshot
                       |                                   |
                 terminal console                 core and lazy evidence

## Validation

Use Parquet fixtures for timezone boundaries, event deduplication, complete totals,
article filtering, replay state, cache reuse, cancellation and progress measurement.
Use fake supervisor processes for lifecycle tests. Run existing dashboard regression
tests and inspect both scoped routes and an untouched route in the local browser.

## Follow-up UI and QA review — 16 Nov 2026

`activity_ui.py:32` splits common and saved filters into separate flex containers;
long catalogue labels repeat coverage text in every option. The console is fixed
across the bottom and can intercept controls on narrow screens. Snapshot cache hits
omit measurements; the JSON size excludes Dash components and is not network size.
Cold investigation reads can replace the shared article catalogue with one title.

Keep callbacks and serving boundaries scoped to these two routes. Use a calendar
with captured-date stepping, one shared compact grid, and a right console that
reserves chart space on desktop and becomes a dismissible right drawer on mobile.
Verify data totals, UI interactions, cache reporting, failure/retry, playback,
article search/context, lifecycle and isolated Kafka controls separately.

## Refresh cadence follow-up — 16 Nov 2026

The one-second `pulse-tick` drove the replay clock, job polling and Kafka polling.
Clock frames selected live snapshots about every 15 seconds and saved replay frames
about every two seconds. Attached Kafka status updates could trigger board queries
more frequently. Even no-op server polling activated the shared loading-pill CSS.
Background core results also temporarily replaced optional evidence with a loader.

Separate the smooth client clock from chart frames, poll jobs only while pending,
and poll Kafka status independently of board queries. Keep existing evidence during
same-scope updates, skip replacing exact cached snapshots, and suppress the shared
loading pill only on the two enhanced routes. Chart cadence must not change replay
speed or event capture.

## Stable title and replay simplification — 16 Nov 2026

`dashboards/app.py` explicitly configured `update_title="Refreshing observations…"`.
Dash's renderer applies that title whenever server work is pending, including health
and progress callbacks. Slower chart cadence therefore could not stop tab-title
changes. Disable that request-title setting; retain in-page progress indicators.

Replay formerly exposed independent saved-playback and Kafka lifecycles as separate
button groups. Present one Start/Stop pair and keep the engine choice, speed and seek
under a closed settings disclosure. Saved Start resumes; its initial/end position
jumps to the first matching captured event. Kafka Start creates an isolated run;
Kafka Stop cancels publication and drains admitted events. Lock processing while a
run is active and stop an old run if its selected scope changes.

## Append-only processing console follow-up

`dashboards/activity_jobs.py:_emit` retains only per-job entries; `activity_ui.py:poll`
replaces the console with that job's log each refresh. `stage_health` exposes five
internal query names instead of explaining ingestion. Actual row, Arrow payload and
query duration counters already exist. The dashboard queries committed event files,
not Kafka directly. Kafka retained data cannot be inferred from cumulative offsets
or Parquet sizes; a read-only low/high watermark probe can show an approximate retained
record count, with explicit unknown bytes and a sampled timestamp.
