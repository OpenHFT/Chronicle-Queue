# AGENTS.md

## Scope
- Chronicle Queue is a Java library built with Maven; sources in `src/main/java`, tests in `src/test/java`.
- CLI tools live in `src/main/java/net/openhft/chronicle/queue/main` and scripts in `bin/`.
- Documentation lives in `README.adoc` and `docs/` (Antora in `docs/antora`).

## Build and test
- Preferred full check:
  - `mkdir -p logs`
  - `mvn verify -l logs/mvn-verify.log`
- Module-scoped example:
  - `mvn -pl <module> -am verify -l logs/mvn-verify.log`
- Test example:
  - `mvn -Dtest=TestClassName test -l logs/mvn-test.log`
- Fast compile:
  - `mvn -DskipTests package -l logs/mvn-skip-tests.log`
- Benchmarks and stress runs (only when asked):
  - `mvn -P run-benchmarks test -l logs/mvn-benchmarks.log`
- Zero cost assertions when needed:
  - `mvn -P assertions test -l logs/mvn-assertions.log`
- Review logs:
  - `rg -n '^\[(WARNING|ERROR)\]|SLF4J\(W\)|\bWARNING:|\bwarning:' logs/mvn-verify.log`
- Do not commit logs/.

## Constraints
- Java baseline: 8 (avoid newer language features).
- Source files must stay ISO-8859-1 (code points 0-255). Prefer ASCII; avoid smart quotes and non-breaking spaces.
- Preserve public APIs, wire formats, and on-disk layouts unless explicitly requested.
- Treat warnings as defects; keep logs clean.
- Avoid extra allocations or synchronisation on hot paths.

## Docs and review checklist
- Add tests for behaviour changes in `src/test/java`; prefer targeted tests over broad suites.
- Update `README.adoc` for user-facing changes.
- Update `docs/antora/modules/command-line` or `docs/utilities.adoc` when CLI tools change.
- For large mechanical changes, declare the transformation rule and keep it consistent.

## References
- Build artefacts live under `target/`; do not commit them.
- `OpenHFT/docs/Company-Wide-Tagging.adoc` for tagging and decision record templates.
