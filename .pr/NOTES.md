# PR: Chronicle-Queue#1099 (CLOSE_FIXED)

**Track D — Successor/regression PR behind a blocked close**  ·  priority tier 3  ·  base `ea`  ·  branch `test/Chronicle-Queue-1099-close-as-fixed`
Issue: https://github.com/OpenHFT/Chronicle-Queue/issues/1099

## Planned change
Close as fixed

## Quality bar (must clear before this PR merges)
- One issue, one PR; link with `Fixes #1099`; scope limited to this issue.
- Regression test that fails before / passes after (re-enable the ignored test where one exists, else add one).
- Cross-repo discipline: Chronicle-Queue exposes integration points only; retention/roll-maintenance policy lives in CQE.
- Do not fork the in-flight QUEUE-143/144 PRs; branch off current `ea` and rebase.
- Docs + changelog updated; author credit retained on any rebase; CI proven green.

## Status
BLOCKED: precondition must clear first (see plan) — this branch scaffolds the successor/test.
_This PR-NOTES commit is the local addressment scaffold; the code change lands on top._
