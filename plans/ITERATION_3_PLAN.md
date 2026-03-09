# Chronicle-Queue Iteration 3 Plan - Cleanup Determinism and Test Hygiene

## Goal

Turn the Iteration 2 feedback corpus into a focused cleanup and test-hygiene pass for Chronicle Queue, while preserving the merged SCQIndexing coverage and avoiding collateral test churn.

## Purpose

Reduce cleanup-related flake and test-support duplication in the Chronicle Queue test suite, using the full Iteration 2 feedback corpus as the prioritisation input.

## Scope

- In scope:
  - shared cleanup/test-support extraction
  - cleanup-sensitive test migration and suppression audit
  - focused validation for the cleanup cluster
  - investigation of the cycle-roll reference-count failure path
- Out of scope:
  - unrelated AIDEW workflow implementation changes
  - broad SCQIndexing production-path rewrites
  - opportunistic test rewrites that are not needed for cleanup determinism

## Inputs

- Feedback source set: all `*FEEDBACK.md` files under `~/ws/`, including the archived `20260309T115700Z` corpus.
- Extended rationale, source inventory, and out-of-scope notes live in `ITERATION_3_PLAN.adoc`.

## Guardrails

- Preserve the merged Iteration 2 SCQIndexing coverage and keep the real `TableStoreWriteLockTest.LockAndHoldUntilInterrupted` subprocess smoke test.
- Prefer explicit resource closure and shared cleanup support over wider timeout inflation or blanket leak-check suppression.
- Do not treat AIDEW workflow/tooling fixes as Chronicle Queue code tasks; track them as external coordination items only.

## Task List

| ID | Status | Task | Acceptance |
|----|--------|------|------------|
| I3-1 | TODO | Baseline the cleanup-flake cluster and suppression inventory. | Record the current `finishedNormally = false` inventory, identify local `drainBackgroundCleanup()` copies, and confirm the current targeted gate command for the cleanup cluster. |
| I3-2 | TODO | Extract shared cleanup support into common test infrastructure. | Add a shared helper in `QueueTestCommon` or a dedicated test-support class for background releaser drain plus bounded cleanup polling; remove the duplicate helper bodies from the current merged tests that now carry them locally. |
| I3-3 | TODO | Migrate the cleanup-sensitive tests to deterministic closure and shared teardown. | `OnReleaseTest` and `AppenderFileHandleLeakTest` use the shared helper; cleanup-sensitive resources are explicitly closed; `preAfter()` is used where background releaser timing or thread-pool shutdown must complete before leak checks. |
| I3-4 | TODO | Add a focused repeatable validation target for the cleanup cluster. | Provide one documented Maven command for repeated local validation of `OnReleaseTest`, `AppenderFileHandleLeakTest`, `RollingChronicleQueueTest`, and the related rolling/resource-cache tests. |
| I3-5 | TODO | Audit the remaining `finishedNormally = false` suppressions and retire the highest-value cases. | Classify each remaining suppression as either removable with deterministic cleanup changes, still blocked on a real product bug, or intentionally deferred with an explicit reason. |
| I3-6 | DISCOVERY | Investigate cycle-roll reference counting in the roll-path cleanup failures. | Reproduce and document the `ChunkedMappedFile` / `StoreAppender.rollCycleTo()` reference-count issue, then decide whether Iteration 3 can fix it or should leave a scoped follow-up with a tighter reproducer. |

## Checklist

- [ ] Baseline the current cleanup-cluster failures and suppressions.
- [ ] Extract one shared cleanup helper into common test support.
- [ ] Migrate the current cleanup-sensitive tests to the shared helper.
- [ ] Add and document a focused cleanup-cluster validation command.
- [ ] Audit and classify the remaining `finishedNormally = false` suppressions.
- [ ] Investigate the cycle-roll reference-count defect and record the outcome.

## Validation Plan

- Targeted cleanup gate:
  - `mvn -Dtest=OnReleaseTest,AppenderFileHandleLeakTest,RollingChronicleQueueTest,RollingResourcesCacheTest test`
- Focused SCQIndexing regression gate:
  - `mvn -Dtest=SCQIndexingTest,SCQIndexingArchTest test`
- Full repository safety gate before close-out:
  - `mvn verify -l target/iteration-3-verify.log`

## Validation

- The focused cleanup gate must pass after the shared-helper migration.
- The focused SCQIndexing regression gate must still pass after the cleanup work.
- The final `mvn verify` run must pass before Iteration 3 is considered complete.

## External Coordination Notes

| ID | Status | Item | Desired outcome |
|----|--------|------|-----------------|
| EXT-AIDEW-1 | TRACK | AIDEW should expose a terminal `COMPLETE` stage after verified `sync-origin`. | After a successful sync, `next` should report a true completion state rather than continuing to present `SYNC_TO_ORIGIN` as both pre-sync and post-sync output. |
| EXT-AIDEW-2 | TRACK | AIDEW should make sync status and validation state easier to inspect. | Clear sync evidence/status output, stronger artifact validation/lint, and less worklist/review noise in future iterations. |
