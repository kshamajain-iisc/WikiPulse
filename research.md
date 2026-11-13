# Dashboard Live Data and UX Research

## Findings

### Why Live preview is empty

- `dashboards.app` starts the web UI and a local Spark session. It does not start Kafka, the EventStreams collector, or the Bronze/window streaming jobs.
- The previous launch used `python -m dashboards.app`, which explains why the UI was available but no live feed was running. The full local stack is launched by `./run-local.sh` / `src.run_local` and requires Docker Kafka plus a valid Wikimedia User-Agent in `.env`.
- `DashboardQueries.recent_edits()` in `src/dashboard_queries.py:100-123` filters the Bronze Parquet table to `ingestion_date >= current_date - 1`. On Nov 12, the local Bronze data last seen on Nov 1 is outside that window, so `recent_edits()` returns `(None, None)`.
- In `DashboardQueries.activity()` (`src/dashboard_queries.py:125-168`), an empty preview falls back to a stored `page_windows_*` table only if one exists. The UI then shows an empty state or historical finalized windows, which made “Live preview” look broken instead of explaining that the stream is stopped or stale.
- The dashboard shell in `dashboards/layout.py:20-55` already refreshes every 30 seconds and has `dcc.Loading`, but its loader is a generic spinner without an in-progress message.

### Existing replay and supervision behavior

- `src/replay/__main__.py` replays historical events into the isolated `wiki.replay.edits` topic. Normal Bronze ingestion in `src/stream_measurements.py:27-39` subscribes only to `KAFKA_TOPIC` (`wiki.raw.edits`), so this replay path is not visible in dashboards.
- `data/live_activity/windows_5m` and `windows_1h` contain persisted finalized windows through Nov 1. These are suitable for an in-UI historical playback mode that does not inject old timestamps into the real-time stream or alter live checkpoints.
- `src/run_local.py:80-156, 200-290` owns child processes and starts Dashboard, Collector, Bronze, window, and detector processes. It currently exits if a child dies; it exposes no health status or restart control to the UI. The runner is the correct owner for restarting its children while preserving their existing checkpoint directories.
- The standalone dashboard can be launched without the runner. Therefore the UI needs to distinguish “supervisor not running” from “stream unhealthy”; its restart control should explain when the complete local stack must be started first.

### Current UI patterns

- `dashboards/layout.py` defines the shared five-page navigation and common window/view/date controls.
- `dashboards/callbacks.py` routes page requests and invokes `DashboardDataService.load()`; `dashboards/data_service.py` serializes Spark work with a lock and caches snapshots for 20 seconds.
- `dashboards/assets/dashboard.css` currently uses green as the primary color, has many 8–12px labels and descriptions, and has no theme switch.
- Page renderers are in `dashboards/pages/`; changing shared controls, status, loading, and theme in the shell avoids duplicating behavior across pages.

## Architecture sketch

```text
EventStreams -> Collector -> Kafka live topic -> Bronze Parquet -> 5m/1h windows -> Dash
                                                        ^                 |
                                                        |                 +-> finalized view
                                                        +---- local stream replay uses persisted windows only

Supervisor (src.run_local) -> owns process health and restart requests
Dash UI (127.0.0.1)        -> reads health and requests a controlled restart
```

## Decisions / constraints

- Keep historical replay separate from live Kafka topics and checkpoints. Use the already persisted finalized window files for dashboard replay.
- Restart stream workers through the supervisor that owns them; do not let a web callback create unmanaged duplicate Spark jobs.
- Preserve the current local-only bind address and checkpoints when implementing the control path.
- Keep live preview as the default, but surface freshness/health clearly. Do not silently label stored data as live.
