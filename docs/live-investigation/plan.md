# Implementation plan

Authorization: the user requested implementation of the scoped report after reviewing
and reducing it to two pages. This plan executes that approved scope.

1. [DONE] Add a bounded asynchronous activity workspace with separate cache/progress
   state, committed-file discovery, projected Arrow reads and accurate row/byte metrics.
2. [DONE] Add complete live and selected-day aggregations, IST/UTC bounds, metadata-driven
   available dates, deduplication and optional article evidence that cannot block core data.
3. [DONE] Add route-scoped controls, skeletons, request-generation guards, processing
   console, retry, frozen snapshots and context-preserving article/back links.
4. [DONE] Add elapsed-time saved replay with pause, seek, restart, speed changes and end
   handling; retain independent rolling and cumulative totals.
5. [DONE] Extend authenticated supervisor commands for idempotent start/stop with drain
   status while leaving the dashboard, broker and controller alive.
6. [DONE] Evaluate and deliver the optional isolated Kafka replay extension without
   modifying detector algorithms or other pages.
7. [DONE] Test exact fixture totals, source boundaries, asynchronous behaviour, lifecycle
   commands and unaffected dashboard routes; validate the running browser and logs.

Implementation adds scoped modules and assets. Legacy page renderers and navigation
remain intact. Existing core activity renderers are reused, with scoped link rewriting
and independently rendered evidence. No new dependencies or frontend framework.

Rollback: remove the scoped registration from the app factory and preserve the legacy
callback path. Stored lake data, production topics and checkpoints are never reset.

## Authorized follow-up: compact controls, right console, complete QA

8. [DONE] Replace saved-date dropdown with a labelled calendar and previous/next captured
   date controls; combine filters into one grid (one live row, two stored rows at
   desktop widths), with responsive wrapping on smaller screens.
9. [DONE] Move the console into a right-side results column; retain manual collapse,
   automatic slow/error expansion, sticky heading, log download and readable metrics.
10. [DONE] Correct cache measurement semantics and result-JSON labels; check article
    discovery after cold article reads.
11. [DONE] Run deterministic feature tests, callback tests, full browser interaction and
    responsive geometry checks. Exercise actual stop/start and isolated Kafka controls,
    document a per-feature result matrix, and leave live capture running on 8080.

12. [DONE] Add a persistent top-toolbar 10s/30s/1m chart-refresh preference, separate
    clock/status/progress polling from chart updates, preserve existing components
    during background refresh, and verify cadence, speed, pause and persistence.

13. [DONE] Disable the automatic browser request title. Replace both replay button
    groups with shared Start/Stop actions, automatic first-event positioning and
    optional collapsed settings. Test saved resume, Kafka drain, title stability,
    first-article capture and the absence of old action buttons.

14. [DONE] Authorized console follow-up: append refresh events to a shared bounded
    deque and rotating local JSONL files under data/run/activity-console (1 MB x 3),
    restore retained history on server restart, and download retained history. Keep
    per-request diagnostic endpoints. Emit one measured completion summary per refresh,
    including cache hits, cancellations and failures. Replace internal stage cards
    with readable ingestion status and sampled broker retention (offset span, not bytes),
    probing in a separate bounded worker. Verify append, restore, bounds, failure,
    cached-zero-work, plain labels, and browser refresh/navigation behavior. Rollback
    consists of removing the history/monitor wiring; event data is unaffected.
