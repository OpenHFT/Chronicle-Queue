# Chronicle-Queue #924 — sub-minute directory-change detection: scope decision & design

Branch: `feat/Chronicle-Queue-924-confirm-a-current-user-needs-sub-i`
Date: 2026-08-19

## Corrected scope (roadmap B6 — WatchService is an OPTIMISATION, not the source of truth)
The original #924 framing ("replace the periodic directory scan with WatchService") is wrong: the
periodic scan is **not** how a Queue normally learns about new cycles. Verified in source:
- **Normal rolls update min/max cycle immediately via `onRoll`** — no scan involved
  (`TableDirectoryListing`, `SingleChronicleQueue`, `StoreAppender` all wire `onRoll`).
- **The interval is a configurable safety net**, not the primary path:
  `SingleChronicleQueueBuilder.forceDirectoryListingRefreshIntervalMs` defaults to `60_000`
  (line 140); consulted at `SingleChronicleQueue.java:1103`
  (`now - lastRefreshTimeMS() >= forceDirectoryListingRefreshIntervalMs`).
- **An explicit out-of-band refresh API already exists** for `.cq4` files changed by another process:
  `ChronicleQueue.refreshDirectoryListing()` (interface line 444), implemented at
  `SingleChronicleQueue.java:345`, and already called from `StoreTailer` and `RefreshMain`.

So the ONLY residual capability #924 could add is **sub-minute automatic detection of files created
or deleted by something OTHER than this Queue instance** (e.g. a sibling process trimming/rolling
`.cq4`s) without the caller invoking `refreshDirectoryListing()` and without waiting up to 60s.

## First increment already landed (green)
`QueueDirectoryWatcher` (a non-blocking `WatchService` wrapper over the queue dir; `hasPendingChange()`
reports whether any `.cq4` was created/deleted since the last poll) + `QueueDirectoryWatcherTest`
(`Tests run: 2, Failures: 0, Skipped: 1` — the skip is the platform-latency-sensitive case). It is a
self-contained building block, deliberately **not** yet wired into `refresh()`.

## Consumer gate (the decision this branch turns on)
Per B6 this is **consumer-gated**: it needs **one named consumer + a maximum acceptable detection
latency**. Without that, the correct action is to **close** — the existing `onRoll` (immediate) +
`refreshDirectoryListing()` (explicit) + 60s fallback already cover every in-process case, and the
interval is user-tunable for the out-of-band case.

Provisional lean (pending the maintainer's consumer answer): **keep the increment as an opt-in
building block; do not wire it into the hot `refresh()` path speculatively.** If a real consumer with
a sub-60s latency requirement is named, promote it per the design below; otherwise close #924 and keep
`QueueDirectoryWatcher` available for that future consumer.

## Design IF a consumer is confirmed
WatchService becomes a **wake-up that triggers the existing full reconciliation**, never the source of
truth, and the periodic fallback is **retained** (Java's WatchService permits `OVERFLOW`, can miss
short-lived files, and gives no remote-FS events):
- On any watch event → call the existing `refresh(true)` full reconciliation (don't trust event
  contents to be complete).
- On `OVERFLOW` → force a full scan.
- Unsupported / remote / network filesystem → silently fall back to the interval (already the default).
- Coalesce bursts of events into a single reconciliation.
- `close()` stops the watcher thread/service; normal `onRoll` rolls still require no scan; the explicit
  `refreshDirectoryListing()` API keeps working unchanged.

### Acceptance criteria (for the promotion PR, once gated)
1. A local create / move / delete of a `.cq4` by another process is visible to a tailer within the
   agreed latency (well under the 60s interval).
2. Injected `OVERFLOW` → a full scan occurs and no change is missed.
3. On an unsupported/remote FS the watcher disables cleanly and interval-driven refresh still works.
4. Event bursts are coalesced into one reconciliation (no scan storm).
5. `close()` stops the watcher; no leaked threads/keys.
6. Regression guard: a normal roll still performs **no** directory scan (assert scan count unchanged).
7. Explicit `refreshDirectoryListing()` still forces an immediate reconciliation.

## The ONE remaining external input
A maintainer/product answer: **is there a named consumer that needs sub-60s detection of externally
changed `.cq4` files, and what is their maximum acceptable latency?** Yes → build per the design above
(the building block is ready). No → close #924; the current mechanisms already suffice.
