# Implementation Plan: Live Preview, Replay, Stream Health, and Dashboard UX

## Approach

Make the live data state explicit, add playback over persisted finalized windows, and let the existing local supervisor report health and handle safe stream restarts. Keep the five dashboard routes and current query boundary. Improve shared shell styling with a non-green blue-violet palette, larger readable type, a persistent light/dark toggle, and a loader with clear progress text.

## Changes Required

### 1. Explain live-preview freshness and support replay

**Files:** `src/dashboard_queries.py`, `dashboards/data_service.py`, `dashboards/layout.py`, `dashboards/callbacks.py`

- Add a distinct Replay view alongside Live preview and Finalized windows.
- Replay saved `data/live_activity/windows_5m` / `windows_1h` from a selected window cursor, with play/pause and speed controls. Replay should advance over stored event-time windows and wrap or stop visibly at the available range; it must not write to Kafka or alter checkpoints.
- Include replay cursor/speed in the service cache key and query filters so each playback frame is computed from a bounded event-time range.
- Add freshness metadata to live-preview responses. If no events exist inside the current 24-hour ingestion horizon, show “No fresh stream data” plus the last observed timestamp and point to the stream-health status. Keep the latest finalized data selectable.

### 2. Add local stream health and controlled restart

**Files:** `src/run_local.py`, `dashboards/app.py`, `dashboards/layout.py`, `dashboards/callbacks.py`, `dashboards/data_service.py`

- Have `src.run_local` publish an atomically replaced local service-state file containing supervisor status, child process states, and last update time. Remove/mark it stopped during shutdown.
- Add a health endpoint/control path in the Dash server. The UI polls it for a compact LIVE / STALE / OFFLINE badge and last-event time.
- Add a “Restart stream” button that sends a one-shot request to the supervisor. The supervisor performs an orderly restart of its stream children (collector, Bronze, 5m/1h windows, detectors) in dependency order and reuses checkpoints; it does not restart Kafka data or clear persisted state.
- If the dashboard is running standalone and no supervisor is present, disable the restart button and show the local start command. Keep external binding restricted to `127.0.0.1`.
- Update controls to show the actual stream state rather than implying an independent live connection.

### 3. Modernize shared dashboard UI

**Files:** `dashboards/layout.py`, `dashboards/callbacks.py`, `dashboards/assets/dashboard.css`, `dashboards/app.py` if an API route is needed

- Replace green accents with a cobalt / blue-violet palette.
- Increase the global base font and the currently very small labels, notes, tables, and navigation text while retaining responsive layouts.
- Add a Light / Dark toggle persisted in browser local storage, with CSS variables for surfaces, text, borders, controls, and chart backgrounds.
- Replace the generic loading spinner with a clear overlay that says which dashboard data is loading and that first Spark queries can take longer.

## Verification

- Start the local dashboard and verify its HTML/health route, live stale/offline message, and theme controls.
- Exercise Replay on the existing local finalized window files and confirm charts/tables change by event time without Kafka writes.
- Start through `./run-local.sh` when Docker prerequisites are available; verify child-health status and restart request handling without deleting checkpoints.
- Inspect all five routes in the browser for legibility, loading feedback, and light/dark rendering. Do not run the full unit suite unless separately requested.

## Rollback

Revert the UI, service, and supervisor changes together. Replay reads remain read-only; restart operations preserve existing data and checkpoint directories.

## Todo

- [x] Implement data freshness messaging and replay cursor service query.
- [x] Add playback controls and callbacks.
- [x] Add atomic supervisor status and restart request handling.
- [x] Add health badge and restart action to the UI.
- [x] Apply palette, font scale, theme toggle, and loader message.
- [x] Verify the live preview, theme controls, loader, and replay playback in the local browser.
- [ ] Verify the full supervised stream and restart action after Docker is available.

## Open Questions

- The UI restart action can restart the locally supervised stream only when the app was launched through `./run-local.sh`; standalone mode will explain how to start the supervisor.
- Live collector operation requires the existing Docker setup and a configured Wikimedia User-Agent. Current dashboard preview data is stale as of Nov 1, so actual real-time data requires the full stack to be running.

## Implementation Log

- Started the standalone dashboard at `http://127.0.0.1:8080`; local finalized windows remain available for replay.
- Verified replay advances through stored windows and changes the displayed event-time metrics.
- `./run-local.sh --check --port 8081` could not start the supervised stream because no container engine is available. The supervised health/restart path remains to be exercised after Docker Desktop is running.
- `py_compile` and `git diff --check` completed successfully. The unit suite was not run.
