# Chronicle Queue CI Data Checklist

**Status:** Draft TODO  
**Scope:** CI data to collect and publish for the `Chronicle-Queue` module

This checklist plans the CI artefacts and data that should be collected for Chronicle Queue so that:

- build and test health is visible over time;
- performance baselines can be tracked;
- static analysis and code quality trends are measurable;
- compliance and documentation work (requirements, decisions, security review) has supporting evidence.

Use this file as a TODO; check items off as the CI pipelines are updated to produce the relevant data.

---

## 1. Build and Environment Metadata

For each CI job that builds `Chronicle-Queue`:

- [ ] Record **Java runtime** details (version, vendor, VM name).
- [ ] Record **OS and architecture** (for example Linux x86_64).
- [ ] Record **Maven version** and key build flags (for example `-Dbuild-all`, `-DskipTests`).
- [ ] Persist a short **build summary** artefact:
  - [ ] Module version and git commit hash.
  - [ ] Branch name and CI build identifier.
  - [ ] Whether the build ran with assertions enabled or disabled.

Data location (to decide and document):

- [x] Decide on a standard path or artefact name for the build summary (for example `ci/build-summary.json` or `ci/build-summary.txt`).
  - Planned path: `Chronicle-Queue/ci/build-summary.json` (see `ci/README.md` for schema).

---

## 2. Test and Coverage Data

For the `Chronicle-Queue` module (and any dependent modules built in the same job):

- [ ] Persist **Surefire** and **Failsafe** XML reports for unit and integration tests.
- [ ] Publish a **test summary** artefact with:
  - [ ] Number of tests run, passed, failed and skipped.
  - [ ] Names of any failing tests, with links into detailed logs.
- [ ] Generate **JaCoCo** coverage reports scoped to Chronicle Queue:
  - [ ] HTML report for interactive browsing.
  - [ ] CSV or XML summary for trend tracking.
- [ ] Capture **coverage thresholds** used in the build (for example `jacoco.line.coverage`, `jacoco.branch.coverage`).

Data location:

- [x] Standardise where JaCoCo artefacts live (for example `Chronicle-Queue/target/site/jacoco` mirrored into CI artefacts).
- [x] Decide on a single place for coverage summaries to be consumed by dashboards (CSV or JSON).
  - Planned location: `Chronicle-Queue/ci/coverage-summary.json` (see `ci/README.md`).

---

## 3. Static Analysis and Code Quality

Static analysis should align with the shared playbooks:

- [QUALITY_PLAYBOOK.md](../QUALITY_PLAYBOOK.md)
- [SONAR_PLAYBOOK.md](../SONAR_PLAYBOOK.md)

Planned CI data:

- [ ] **Checkstyle**
  - [ ] Run Checkstyle for Chronicle Queue with the shared baseline configuration.
  - [ ] Export XML or text reports as CI artefacts.
  - [ ] Produce a small summary (for example counts per rule or rules with non-zero violations).
- [ ] **SpotBugs**
  - [ ] Run the low-violation profile for Chronicle Queue.
  - [ ] Persist XML or HTML reports.
  - [ ] Emit a summary mapping bug patterns to counts (for example `QUEUE-OPS-*` style IDs once defined).
- [ ] **Sonar / SonarCloud** (where configured)
  - [ ] Capture the key metric snapshot (bugs, vulnerabilities, code smells, coverage, duplication).
  - [ ] Store the corresponding JSON response or a condensed summary in CI artefacts.

Data location:

- [x] Decide on a consistent directory layout for static analysis artefacts (for example `ci/checkstyle`, `ci/spotbugs`, `ci/sonar`).
  - Planned locations are documented in `Chronicle-Queue/ci/README.md`.

---

## 4. Performance and Benchmark Data

Chronicle Queue has explicit performance requirements (`QUEUE-NF-P-*`) and benchmark documentation in `docs/performance.adoc`.
CI should collect enough data to spot regressions, even if heavy benchmarks run only on selected agents.

- [ ] Identify a **minimal smoke benchmark** suite that can run on CI regularly (for example reduced-scale JLBH or JMH tests).
- [ ] For full benchmarks (run nightly or on demand):
  - [ ] Capture configuration (hardware, JVM flags, dataset sizes).
  - [ ] Persist benchmark results (latency percentiles, throughput) in a machine-readable format (CSV or JSON).
- [ ] Tag benchmark results with:
  - [ ] `QUEUE-NF-P-*` requirement IDs they relate to.
  - [ ] Chronicle Queue version and git commit.

Data location:

- [x] Choose a standard path for benchmark artefacts (for example `ci/performance/chronicle-queue`).
  - Planned base directory: `Chronicle-Queue/ci/performance/` with JSON files as described in `ci/README.md`.

---

## 5. Compliance and Documentation Evidence

Several Chronicle-wide documents rely on CI data as evidence:

- `ARCH_TODO.md`, `ARCHITECTURE_RESEARCH_GUIDE.md`
- `COMPLIANCE_QUICK_REFERENCE.md`
- `COMPLIANCE_UPDATE_SUMMARY.md`

For Chronicle Queue specifically:

- [ ] Link **project requirements** (`QUEUE-FN-*`, `QUEUE-NF-*`) to:
  - [ ] Test suites (via test reports and coverage).
  - [ ] Benchmarks (for performance requirements).
- [ ] Link **decision records** (`CQ-*` in `decision-log.adoc`) to:
  - [ ] Static analysis reports (for example decisions about suppressions).
  - [ ] CI build summaries that show which checks ran.
- [ ] Capture a lightweight **compliance snapshot** per CI build:
  - [ ] Which of the main checks ran (tests, coverage, Checkstyle, SpotBugs, Sonar, benchmarks).
  - [ ] Pass/fail status per category.

Data location:

- [x] Define a simple JSON or YAML format for the compliance snapshot (for example `ci/chronicle-queue-compliance.json`).
  - Planned path and example fields documented in `Chronicle-Queue/ci/README.md` under “Compliance Snapshot”.

---

## 6. Pipeline Inventory and Ownership

To make CI data discoverable:

- [ ] List all CI pipelines that build or test Chronicle Queue (names, triggers, branches).
- [ ] For each pipeline, record:
  - [ ] The commands it runs (for example `mvn -pl Chronicle-Queue -am verify`).
  - [ ] Which artefacts it publishes (and where).
- [ ] Nominate an **owner** or contact for each pipeline to review data quality and update CI when requirements change.

Once this checklist is implemented:

- [ ] Update `Chronicle-Queue/TODO.md` and any relevant sections in `ARCH_TODO.md` to reference this file and mark CI data collection tasks as complete.
