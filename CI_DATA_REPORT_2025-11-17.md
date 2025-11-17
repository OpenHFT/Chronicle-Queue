# Chronicle Queue – CI Data Snapshot (2025-11-17)

**Module:** Chronicle-Queue  
**Artifact ID:** `chronicle-queue`  
**Version (pom.xml):** `5.27ea12-SNAPSHOT`  
**Parent:** `net.openhft:java-parent-pom:1.27ea2-SNAPSHOT`

This report is a **one-off snapshot** describing what CI-related data is currently available (from configuration and documentation) for Chronicle Queue, and where future CI runs should publish richer artefacts.
It does **not** represent an actual CI build execution; paths and commands are based on the project structure as-of 2025-11-17.

---

## 1. Build and Configuration

- **Build tool:** Maven (module-level POM `Chronicle-Queue/pom.xml`).
- **Jacoco coverage thresholds (from POM properties):**
  - `jacoco.line.coverage` = `0.788`
  - `jacoco.branch.coverage` = `0.713`
- **Surefire configuration:**
  - `forkCount = 1`
  - `argLine = ${surefireArgLine} ${jvm.requiredArgs}` (Jacoco agent and JPMS flags propagated from parent).
- **Standard build commands (recommended for CI):**
  - Module-scoped verify:  
    `mvn -pl Chronicle-Queue -am verify`
  - Full reactor (if needed):  
    `mvn -Dbuild-all=true clean verify`

At the time of this snapshot there is no `Chronicle-Queue/target/` directory in the workspace, so no actual compiled/test artefacts or coverage reports are present yet.

---

## 2. Tests and Coverage (Planned)

**Current state (local workspace):**

- No `Chronicle-Queue/target/surefire-reports` or `Chronicle-Queue/target/site/jacoco` directories exist.
- No test or coverage data has been collected in this run; only configuration is available.

**Planned behaviour for CI:**

- **Test execution:**
  - Run Maven tests for Chronicle-Queue (and dependencies) via `mvn -pl Chronicle-Queue -am test` or `verify`.
  - Surefire/Failsafe XML reports under:
    - `Chronicle-Queue/target/surefire-reports/`
    - `Chronicle-Queue/target/failsafe-reports/` (if integration tests are defined).
- **Coverage:**
  - JaCoCo HTML and CSV under:
    - `Chronicle-Queue/target/site/jacoco/`
  - Coverage thresholds enforced via the POM properties above.

**Planned summary artefacts (see `Chronicle-Queue/ci/README.md`):**

- `Chronicle-Queue/ci/test-summary.json` – aggregated counts (run/passed/failed/skipped) and a list of failing tests.
- `Chronicle-Queue/ci/coverage-summary.json` – percentages for line/branch/instruction/method coverage and thresholds.

---

## 3. Static Analysis and Code Quality (Planned)

Static analysis is governed by workspace-wide playbooks:

- `QUALITY_PLAYBOOK.md`
- `SONAR_PLAYBOOK.md`

**Current state (local workspace):**

- No Chronicle-Queue-specific Checkstyle or SpotBugs reports exist in this workspace snapshot.
- Sonar/SonarCloud metrics are not recorded locally; any existing dashboards live in external systems.

**Planned CI behaviour:**

- **Checkstyle:**
  - Run against Chronicle-Queue using the shared baseline from `Chronicle-Quality-Rules`.
  - Persist reports under `Chronicle-Queue/ci/checkstyle/` (for example `checkstyle.xml`).
- **SpotBugs:**
  - Run the low-violation profile on Chronicle-Queue.
  - Persist reports under `Chronicle-Queue/ci/spotbugs/` (for example `spotbugs.xml`).
- **Sonar/SonarCloud:**
  - Collect a metric snapshot via the Sonar API and store a condensed JSON response under `Chronicle-Queue/ci/sonar/`.

This report therefore documents **what should be collected**, not current findings.

---

## 4. Performance and Benchmarking

Chronicle Queue is performance-critical, with requirements and documentation in:

- `Chronicle-Queue/docs/performance.adoc`
- `Chronicle-Queue/src/main/docs/project-requirements.adoc` (`QUEUE-NF-P-*`)

**Current state (local workspace):**

- No benchmark artefacts or timing logs exist under `Chronicle-Queue/ci/` or `Chronicle-Queue/target/` for this snapshot.

**Planned CI behaviour:**

- Define a small, repeatable “smoke benchmark” suite suitable for CI.
- Run heavier benchmarks (for example JMH/JLBH harnesses) on a scheduled basis.
- Store machine-readable benchmark results under:
  - `Chronicle-Queue/ci/performance/`

Each benchmark result should:

- identify the benchmark name and configuration;
- record throughput and latency metrics;
- reference the relevant `QUEUE-NF-P-*` requirement IDs;
- record module version, git commit and CI build identifier.

---

## 5. Requirements, Decisions and Compliance Context

Chronicle Queue now has the following documentation artefacts:

- `src/main/docs/project-requirements.adoc` – functional and non-functional requirements:
  - `QUEUE-FN-*` – core behaviour (append-only durability, tailers, random index access, builder configuration, Wire integration).
  - `QUEUE-NF-P-*` – performance targets.
  - `QUEUE-NF-S-*`, `QUEUE-NF-O-*` – security and operability constraints.
- `src/main/docs/decision-log.adoc` – key architectural and operational decisions (`CQ-*`).
- `src/main/docs/security-review.adoc` – security posture and boundaries (encryption, off-heap safety, retention responsibilities).
- `src/main/docs/testing-strategy.adoc` – unit/integration/performance testing strategy and linkage back to requirements and decisions.

The CI data planned in `Chronicle-Queue/CI_DATA_CHECKLIST.md` and `Chronicle-Queue/ci/README.md` is intended to:

- provide evidence that `QUEUE-FN-*` and `QUEUE-NF-*` requirements are exercised by tests and benchmarks;
- show which decisions (`CQ-*`) are enforced or monitored by static analysis and run-time checks;
- support higher-level compliance documents (`ARCHITECTURE_RESEARCH_GUIDE.md`, `COMPLIANCE_QUICK_REFERENCE.md`, `COMPLIANCE_UPDATE_SUMMARY.md`).

This snapshot records that the **structure and documentation** for such evidence are in place, while actual CI pipelines still need to be wired to produce and upload the data described.
