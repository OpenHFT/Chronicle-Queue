# Chronicle Queue CI Data Artefacts

This directory documents where CI pipelines should write data for the `Chronicle-Queue` module.
It supports the checklist in `../CI_DATA_CHECKLIST.md`.

## 1. Build Summary

- Path: `Chronicle-Queue/ci/build-summary.json`
- Contents (JSON object):
  - `module`: `"Chronicle-Queue"`
  - `version`: Maven project version (for example `5.27ea12-SNAPSHOT`)
  - `gitCommit`: full git commit hash
  - `gitBranch`: branch or ref name
  - `ciBuildId`: CI-specific build identifier (for example TeamCity build ID)
  - `javaRuntime`: Java version string (for example output of `java -version`)
  - `osName`: operating system name
  - `osArch`: CPU architecture
  - `mavenCommand`: the Maven command used (for example `mvn -pl Chronicle-Queue -am verify`)
  - `assertionsEnabled`: `true`/`false` if the test JVMs ran with assertions enabled

CI pipelines should populate this file once per build of Chronicle Queue.

## 2. Test and Coverage Summaries

- Surefire/Failsafe XML reports:
  - Use Maven defaults under `Chronicle-Queue/target/surefire-reports` and `Chronicle-Queue/target/failsafe-reports`.
  - CI should archive these directories as artefacts.

- Test summary:
  - Path: `Chronicle-Queue/ci/test-summary.json`
  - Suggested fields:
    - `testsRun`, `testsPassed`, `testsFailed`, `testsSkipped`
    - `failingTests`: array of objects with `className` and `methodName`

- Coverage:
  - HTML + CSV from JaCoCo:
    - Path (from Maven): `Chronicle-Queue/target/site/jacoco`
  - Coverage summary:
    - Path: `Chronicle-Queue/ci/coverage-summary.json`
    - Suggested fields:
      - `lineCoverage`, `branchCoverage`, `instructionCoverage`, `methodCoverage` (percentages)
      - thresholds from `jacoco.line.coverage` and `jacoco.branch.coverage`

## 3. Static Analysis

CI jobs that run static analysis should write data into:

- Checkstyle:
  - Reports: `Chronicle-Queue/ci/checkstyle/checkstyle.xml` (or `.txt` for human-readable).
- SpotBugs:
  - Reports: `Chronicle-Queue/ci/spotbugs/spotbugs.xml` (and optionally HTML).
- Sonar/SonarCloud:
  - Snapshot: `Chronicle-Queue/ci/sonar/sonar-metrics.json` (REST response or condensed summary).

## 4. Performance and Benchmarks

For performance tests relating to `QUEUE-NF-P-*` requirements:

- Path: `Chronicle-Queue/ci/performance/`
- Suggested files:
  - `smoke-benchmarks.json` for lightweight CI benchmarks.
  - `full-benchmarks.json` for nightly or dedicated runs.

Each entry should record:

- `benchmarkName`
- `config` (hardware, JVM flags, dataset sizes)
- `metrics` (throughput, latency percentiles)
- `requirements` (array of `QUEUE-NF-P-*` IDs)
- `version`, `gitCommit`, `ciBuildId`

## 5. Compliance Snapshot

- Path: `Chronicle-Queue/ci/chronicle-queue-compliance.json`
- Suggested fields:
  - `tests`: `{ "ran": true|false, "passed": true|false }`
  - `coverage`: `{ "ran": true|false, "passedThresholds": true|false }`
  - `checkstyle`: `{ "ran": true|false, "clean": true|false }`
  - `spotbugs`: `{ "ran": true|false, "clean": true|false }`
  - `sonar`: `{ "ran": true|false, "qualityGate": "OK"|"WARN"|"FAIL" }`
  - `benchmarks`: `{ "ran": true|false }`
  - `requirementsVersion`: version string for `project-requirements.adoc`
  - `decisionLogVersion`: version string for `decision-log.adoc`

The exact schema can evolve, but keeping the location and intent stable allows dashboards and compliance tooling to consume the data reliably.

