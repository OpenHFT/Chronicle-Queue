# PR: Chronicle-Queue#1150 (VALID_BUG)

**Track B — New fix PR (VALID_BUG)**  ·  priority tier 3  ·  base `ea`  ·  branch `fix/Chronicle-Queue-1150-make-multiple-method-reader-exclus`
Issue: https://github.com/OpenHFT/Chronicle-Queue/issues/1150

## Planned change
Make multiple method-reader exclusion regexes combine consistently (all apply, matching text-reader); re-enable ignored test.

## Quality bar (must clear before this PR merges)
- One issue, one PR; link with `Fixes #1150`; scope limited to this issue.
- Regression test that fails before / passes after (re-enable the ignored test where one exists, else add one).
- Cross-repo discipline: Chronicle-Queue exposes integration points only; retention/roll-maintenance policy lives in CQE.
- Do not fork the in-flight QUEUE-143/144 PRs; branch off current `ea` and rebase.
- Docs + changelog updated; author credit retained on any rebase; CI proven green.

## Status
Ready to implement on this branch.
_This PR-NOTES commit is the local addressment scaffold; the code change lands on top._
