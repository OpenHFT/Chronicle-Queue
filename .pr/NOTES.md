# PR: Chronicle-Queue#1096 (VALID_BUG)

**Track B — New fix PR (VALID_BUG)**  ·  priority tier 2  ·  base `ea`  ·  branch `fix/Chronicle-Queue-1096-fallback-eof-write-must-use-normal`
Issue: https://github.com/OpenHFT/Chronicle-Queue/issues/1096

## Planned change
Fallback EOF write must use normal-path padding/position; force tryReserve failure in a regression test and compare EOF bytes.

## Quality bar (must clear before this PR merges)
- One issue, one PR; link with `Fixes #1096`; scope limited to this issue.
- Regression test that fails before / passes after (re-enable the ignored test where one exists, else add one).
- Cross-repo discipline: Chronicle-Queue exposes integration points only; retention/roll-maintenance policy lives in CQE.
- Do not fork the in-flight QUEUE-143/144 PRs; branch off current `ea` and rebase.
- Docs + changelog updated; author credit retained on any rebase; CI proven green.

## Status
Ready to implement on this branch.
_This PR-NOTES commit is the local addressment scaffold; the code change lands on top._
