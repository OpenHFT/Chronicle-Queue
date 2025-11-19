# Chronicle-Queue - Repository TODO

**📋 Part of:** [Chronicle Architecture Documentation](../ARCH_TODO.md)
**Module Layer:** Layer 1 (Data Structures & Serialization)
**Priority:** 🔴 P0
**Last Updated:** 2025-11-18

## Purpose

This TODO file tracks work specific to Chronicle-Queue that feeds into the master [ARCH_TODO.md](../ARCH_TODO.md). It helps break down the architecture documentation work into manageable, repository-specific chunks.

## Related Main TODO Files

- [../ARCH_TODO.md](../ARCH_TODO.md) - Master architecture documentation roadmap
- [../TODO_INDEX.md](../TODO_INDEX.md) - Index of all TODO files
- [../ADOC_TODO.md](../ADOC_TODO.md) - AsciiDoc standardization (affects this module)

## Module Information for Architecture Overview

### Basic Information
- [x] **Module Name:** Chronicle-Queue
- [x] **Maven Artifact ID:** chronicle-queue
- [x] **Primary Purpose:** Broker-less, off-heap, persisted messaging between JVMs with microsecond-level latencies and durable replay for logging, IPC and audit/compliance use cases.
- [x] **Layer in Chronicle Stack:** Layer 1 (Data Structures & Serialization)
- [x] **Dependencies (Chronicle modules):** Chronicle-Core, Chronicle-Bytes, Chronicle-Wire, Chronicle-Threads (plus optional Affinity and JLBH for pinning and benchmarks).
- [x] **Key Classes/Interfaces:** `ChronicleQueue`, `ExcerptAppender`, `ExcerptTailer`, `SingleChronicleQueueBuilder`, `RollingChronicleQueue`.

### ISO Alignment and Trust Zone

- [x] **Trust zone identified (Edge/Core/Foundation):** Chronicle-Queue operates in the *Core (Zone B)* as defined in `Chronicle-Quality-Rules/src/main/docs/architectural-standards.adoc`, providing durable, append-only storage for messages that are assumed to have passed Edge validation in Network/Wire or other ingress layers.
- [x] **Shared standards reviewed:** `src/main/docs/security-review.adoc`, `architecture-overview.adoc` and the requirements/docs now align with the shared standards in `Chronicle-Quality-Rules/src/main/docs/security-review.adoc`, `architectural-standards.adoc` and `operations-guide.adoc`, explicitly documenting Chronicle-Queue’s responsibilities for persistence, replay and data-retention constraints.

### Architecture Information for ARCH_TODO.md Stage 3

**Feeds into:** ARCH_TODO.md Stage 3 - Module Deep Dives (ARCH-MOD-QUEUE)

- [x] **Core Abstractions:** Append-only, roll-cycled queue files; tiered indexing over `.cq4` data; `ExcerptAppender` / `ExcerptTailer` for sequential write/read; builder-based configuration via `SingleChronicleQueueBuilder`.
- [x] **Interactions with other modules:** Uses Chronicle-Bytes for off-heap and memory-mapped storage, Chronicle-Wire for marshalling messages to/from `Bytes`, Chronicle-Threads for pretouch/event-loop support, and Chronicle-Core for OS/JVM utilities and system properties.
- [x] **Typical use cases:** Low-latency IPC between microservices, journalling and audit logging (“record everything” store), market-data and order-flow capture in trading systems, and durable async processing pipelines.
- [x] **Performance characteristics:** Designed for sub-microsecond same-host write/read latencies, millions of messages per second per thread, and sub-10µs replicated messaging in Chronicle-Queue-Enterprise (per README/FAQ benchmarks).
- [x] **Design patterns used:** Off-heap and memory-mapped storage, single-writer principle for appenders, flyweight-style tailers over shared data, builder pattern for configuration, background pretoucher to hide paging costs, and append-only log with time-based roll cycles.

### Existing Documentation Audit

- [x] Check if `src/main/docs/architecture-overview.adoc` exists
  - [x] If yes: Review quality (compare to Chronicle-Bytes standard) - `src/main/docs/architecture-overview.adoc` now consolidates high-level architecture and should be kept in sync with `README.adoc` and `docs/How_it_works.adoc`.
  - [x] If no: Note as gap for ARCH_TODO Stage 5.5 - N/A, the file now exists and no gap needs to be recorded.
- [x] Check if `src/main/docs/project-requirements.adoc` exists
  - [x] If yes: Review for ARCH_TODO Stage 1.75 (Requirements Overview) – `src/main/docs/project-requirements.adoc` now exists and captures initial `QUEUE-FN-*` and `QUEUE-NF-*` requirements with verification notes.
  - [x] If no: Note as gap for FUNC_TODO.md - N/A, requirements have been introduced and this gap has been closed.
- [x] Check if `src/main/docs/decision-log.adoc` exists
  - [x] If yes: Review for ARCH_TODO Stage 1.85 (Decision Log Overview) - `src/main/docs/decision-log.adoc` exists and documents key decisions (`CQ-FN-101`, `CQ-FN-102`, `CQ-NF-P-201`, `CQ-NF-S-301`, `CQ-NF-O-401`) with context, alternatives and links to `docs/`.
  - [x] If no: Note as gap for DECISION_TODO.md – N/A, the decision log exists and is in active use.
- [x] Check if `README.adoc` provides good module overview
- [x] Check if `AGENTS.md` exists and follows canonical template - no module-local `AGENTS.md` yet; see root `canonical-AGENTS.md` and TODO.md Section 2 for rollout.

### Documentation Gaps (for ARCH_TODO Stage 5.5)

**Missing Documentation:**
- [x] Architecture overview? [N] `src/main/docs/architecture-overview.adoc` now exists and consolidates the high-level architecture previously split between `README.adoc` and `docs/How_it_works.adoc`.
- [x] Requirements documentation? [N] `src/main/docs/project-requirements.adoc` now exists and captures an initial set of `QUEUE-FN-*` and `QUEUE-NF-*` requirements.
- [x] Decision log? [N] `src/main/docs/decision-log.adoc` exists and follows the Nine-Box pattern for key CQ decisions.
- [x] Security review? [N] `src/main/docs/security-review.adoc` now exists and consolidates queue-specific security considerations and boundaries.
- [x] Testing strategy? [N] `src/main/docs/testing-strategy.adoc` now documents the unit, integration and performance testing approach for Chronicle-Queue.
- [x] Performance targets? [Y] Performance characteristics appear in `README.adoc`, `docs/performance.adoc` and benchmarks; `project-requirements.adoc` introduces initial `QUEUE-NF-P-*` entries but a fuller catalogue and cross-links still need to be built.

**Documentation Quality Issues:**
- [x] Missing `:toc:`, `:lang: en-GB`, or `:source-highlighter: rouge`? Main narrative docs under `src/main/docs` and key guides under `docs/` now include the standard front-matter; small one-screen notices (for example portal pointers) remain minimal by design.
- [x] Manual section numbering instead of `:sectnums:`? No manual numeric prefixes were found in Chronicle-Queue `.adoc` files outside Antora content.
- [x] Broken cross-references? Non-Antora links between `README.adoc`, `docs/*.adoc` and `src/main/docs/*.adoc` were checked and point to existing files.
- [ ] Outdated information?

## Requirements for Architecture Overview (ARCH_TODO Stage 1.75)

**Feeds into:** Requirements Overview consolidation

- [x] **Identify key functional requirements:** Persist all appended messages durably to disk-backed queue files; provide ordered, forward-only reads via `ExcerptTailer`; support random access by index; support rolling of queue files by time cycle; expose builders to configure location, wire type, roll cycle and indexing.
- [x] **Identify key non-functional requirements:**
  - [x] Performance targets: Sub-microsecond same-host append/tail latencies and millions of messages per second per thread; bounded tail latency under sustained load, as documented in `README.adoc` and `docs/performance.adoc`.
  - [x] Security obligations: Safe handling of off-heap and memory-mapped data via Chronicle-Bytes; avoidance of unbounded memory growth; documented responsibility boundaries for encryption (`CQ-NF-S-301` leaves at-rest encryption to higher layers or filesystem).
  - [x] Operability requirements: Provide system-properties for tuning behaviour (`docs/systemProperties.adoc`); expose metrics such as slow appender warnings and scan latency; support roll-cycle based retention strategies without automatic deletion.
- [x] **Map requirements to architecture patterns:** Append-only log files plus tiered indexing implement durability and random access; time-based roll cycles support operability and retention requirements; Single Writer-style appenders and flyweight tailers enforce concurrency and performance goals; reliance on Chronicle-Bytes and Wire fulfils off-heap and marshalling requirements.

## Decisions for Architecture Overview (ARCH_TODO Stage 1.85)

**Feeds into:** Decision Log Overview consolidation

- [x] **Identify key architectural decisions:**
  - [x] Decision ID (if in decision-log.adoc): `CQ-FN-101`, `CQ-FN-102`, `CQ-NF-P-201`, `CQ-NF-S-301`, `CQ-NF-O-401`.
  - [x] Brief description: `CQ-FN-101` defines the queue file format and time-based roll-cycle model; `CQ-FN-102` describes the appender/tailer concurrency model; `CQ-NF-P-201` captures durability, flush policy and recovery expectations; `CQ-NF-S-301` documents the encryption responsibility boundary; `CQ-NF-O-401` describes retention and roll-file cleanup as operational concerns.
  - [x] Rationale: Time-based roll cycles align with operational boundaries; single-writer-style appenders avoid hidden synchronisation; durability and flush policies balance latency against recovery guarantees; keeping encryption and retention as explicit operational choices avoids surprising defaults in regulated environments.
  - [x] Alternatives considered: Monolithic or purely size-based queue files; fully thread-safe appenders; synchronous flush on every write; built-in automatic deletion of roll files; default on-disk encryption in the core library.
- [x] **Identify decision patterns used:**
  - [x] Off-heap memory? [Y] Queue storage is implemented over Chronicle-Bytes (`MappedBytes`/`NativeBytes`) and memory-mapped files to avoid GC pauses and support huge queues.
  - [x] Single writer principle? [Y] `CQ-FN-102` encourages one appender per writer thread with explicit external synchronisation if sharing, aligning with the Single Writer Principle.
  - [x] Reference counting? [Y] Chronicle-Queue relies on Chronicle-Bytes `ReferenceCounted` primitives for off-heap resources; queue code expects callers to respect `Closeable`/`ReferenceCounted` lifecycles.
  - [x] Flyweight pattern? [Y] Tailers and appender views act as flyweights over the underlying queue storage, reusing buffers and avoiding per-message allocation where possible.

## Glossary Terms (ARCH_TODO Stage 1.5)

**Feeds into:** Cross-module glossary

- [x] **Module-specific terms to include in glossary:**
  - [x] Term 1: Appender - writer-side view over a Chronicle Queue (`ExcerptAppender`) used to append messages sequentially to the end of the queue.
  - [x] Term 2: Tailer - reader-side view over a Chronicle Queue (`ExcerptTailer`) used to read messages in order, advancing its own cursor independently from other tailers.
  - [x] Term 3: Roll cycle - time-based policy that controls how queue data is partitioned into roll files (for example daily or hourly), influencing file naming, retention and recovery behaviour.


## ISO 9001 Quality Management Considerations

**Reference:** [../COMPLIANCE_QUICK_REFERENCE.md](../COMPLIANCE_QUICK_REFERENCE.md)

### Design Inputs (ISO 9001 Clause 8.3.3)
- [x] **Functional requirements documented?** Yes - `src/main/docs/project-requirements.adoc` now defines an initial set of `QUEUE-FN-*` requirements derived from existing documentation and tests.
  - [x] Location: `src/main/docs/project-requirements.adoc`
  - [x] Requirements use Nine-Box taxonomy? (QUEUE-FN-NNN)
  - [x] Requirements are testable and verifiable? Each requirement includes a verification description, with additional detail in `src/main/docs/testing-strategy.adoc`.
- [x] **Non-functional requirements documented?** Partially - performance, security and operability expectations are now captured in `QUEUE-NF-P-*`, `QUEUE-NF-S-*` and `QUEUE-NF-O-*` entries in `project-requirements.adoc`, with additional narrative in `README.adoc`, `docs/performance.adoc`, `docs/systemProperties.adoc` and decision-log entries.
  - [x] Performance requirements (QUEUE-NF-P-NNN)
  - [x] Security requirements (QUEUE-NF-S-NNN)
  - [x] Operability requirements (QUEUE-NF-O-NNN)

### Design Outputs (ISO 9001 Clause 8.3.5)
- [x] **Architecture documented?** Yes - `src/main/docs/architecture-overview.adoc` now provides the canonical overview, with `README.adoc` and `docs/How_it_works.adoc` as supporting material.
  - [x] Location: `src/main/docs/architecture-overview.adoc`
  - [x] Describes key components and their interactions?
  - [x] Includes interface specifications? (High-level API roles, queue file structure and builder configuration are described.)
- [x] **APIs and interfaces specified?**
  - [x] Public API documented (JavaDoc)? Public APIs are documented in the published JavaDoc (see the badge link in `README.adoc`) and in `src/main/docs/architecture-overview.adoc`.
  - [x] Integration points with other modules described? Interactions with Chronicle-Core, Chronicle-Bytes, Chronicle-Wire, Chronicle-Threads and Chronicle-Queue-Enterprise are documented in `architecture-overview.adoc` and `README.adoc`.

### Design Verification (ISO 9001 Clause 8.3.4)
- [x] **Requirements traceable to tests?**
  - [x] Test classes reference requirement IDs in comments/docs? Test sources do not embed `QUEUE-*` identifiers directly, but `src/main/docs/project-requirements.adoc` now includes a `Requirements-to-tests mapping` table that links each documented `QUEUE-FN-*` and `QUEUE-NF-*` requirement to specific test classes from the `chronicle-queue-*-tests.jar` (for example `ExcerptAppenderTest`, `TailerDirectionTest`, `ChronicleQueueIndexTest`, `RollCyclesTest`, `ChronicleQueueMethodsWithoutParametersTest` and related stress tests).
  - [x] Coverage: What % of requirements have corresponding tests? All requirements currently listed in `project-requirements.adoc` have at least one representative unit test, integration test or benchmark referenced in that mapping table, so the documented `QUEUE-*` set has effectively 100% traceability to executable checks; additional requirements added in future should follow the same pattern.
- [x] **Test strategy documented?**
  - [x] Unit test approach
  - [x] Integration test approach
  - [x] Performance test approach (if applicable)
- [x] **Code review evidence?**
  - [x] PR review process followed? Chronicle-Queue follows the standard GitHub pull-request process described in the root and module `AGENTS.md` files, including mandatory review and `mvn -q clean verify` before merge; spot-checks of recent Chronicle-Queue changes confirm that substantive work landed via reviewed PRs rather than direct pushes.
  - [x] Review comments addressed? Review discussions are captured in the Git hosting platform and, where they result in architectural or behavioural changes, are linked back to `CQ-*` decision-log entries or `QUEUE-*` requirements; no open, unaddressed review threads are currently known for this module.

### Design Changes (ISO 9001 Clause 8.3.4)
- [x] **Architectural decisions documented?**
  - [x] Location: `src/main/docs/decision-log.adoc`
  - [x] Decisions include context, alternatives, rationale? (See `CQ-FN-101`, `CQ-FN-102`, `CQ-NF-P-201`, `CQ-NF-S-301`, `CQ-NF-O-401`.)
  - [x] Impact of changes assessed? (Each record discusses operational and behavioural consequences.)
- [x] **Change history maintained?**
  - [x] Git commit messages describe rationale? Commit-message guidance from the root `AGENTS.md` is applied here (imperative subject, brief rationale and impact); a spot review of recent Chronicle-Queue commits shows descriptive messages that reference features, bug fixes or performance work, with breaking or high-impact changes explained in more detail.
  - [x] Breaking changes documented in release notes? Chronicle-Queue uses semantic versioning and release notes in the parent project to call out file-format or API changes; where queue semantics or compatibility are affected, corresponding `CQ-*` decision-log entries and `QUEUE-*` requirements are updated to keep the history discoverable from within this repository.

## ISO 27001 Information Security Considerations

**Reference:** [../ARCHITECTURE_RESEARCH_GUIDE.md](../ARCHITECTURE_RESEARCH_GUIDE.md) - Security Research Topics

### Secure Coding (ISO 27001 Control A.8.28)
- [ ] **Input validation implemented?**
  - [ ] Where are untrusted inputs received? Chronicle-Queue reads data from memory-mapped `.cq4` files and from application code writing via `ExcerptAppender`; when queues are shared between processes or hosts, appenders must treat on-disk structures as potentially corrupted and rely on header and index checks before trusting lengths and offsets.
  - [ ] How are malformed inputs handled? `security-review.adoc` should document how Chronicle-Queue reacts to truncated data, invalid headers or inconsistent indices (for example failing fast on open, skipping damaged regions, or halting with an explicit error) so operators understand the trade-offs.
  - [ ] Size limits enforced? Configuration and code paths should enforce sensible size limits for messages and queue segments; where limits are enforced by Bytes/Memory-mapping primitives, this should be called out in the security review.
- [x] **Bounds checking implemented?**
  - [x] Buffer overflow prevention mechanisms? Chronicle-Queue relies on Chronicle-Bytes for buffer bounds checking; queue-specific logic must avoid reading or writing beyond the declared capacities or roll-cycle boundaries, as described in `security-review.adoc`.
  - [x] Array access validation? Any in-memory arrays used for indexing or metadata rely on JVM bounds checks; manual index arithmetic is avoided in favour of safe abstractions where feasible.
  - [x] Off-heap memory bounds checked? Off-heap and memory-mapped regions are wrapped in `BytesStore`/`Bytes` views with enforced capacities; callers are expected not to bypass these abstractions except in carefully reviewed hot-path code.
- [x] **Static analysis performed?**
  - [x] Checkstyle violations reviewed? The latest Java 21 quality runs show Checkstyle clean for Chronicle-Queue; any future violations must be reviewed and resolved or justified.
  - [x] SpotBugs security patterns checked? SpotBugs currently reports zero bugs for this module under the shared configuration; maintaining this status and documenting any future suppressions is part of ongoing security hygiene.
  - [x] Suppressions justified and documented? Any SpotBugs suppressions related to queue persistence or IPC are expected to be justified with a short comment and, where relevant, a note in `security-review.adoc`.

### Access Control (ISO 27001 Control A.8.3)
- [x] **Access restrictions implemented?**
  - [x] Are there authentication/authorization mechanisms? Chronicle-Queue itself does not implement authentication or authorisation; access to queue files is governed by operating system user and group permissions, as described in `security-review.adoc`.
  - [x] If yes, where and how are they implemented? Application-level services that use Chronicle-Queue are expected to enforce their own authorisation rules and ensure that only appropriate processes have file-system access to queue directories.
  - [x] Principle of least privilege followed? Deployment guidance recommends running queue processes under dedicated users with restricted access to queue directories and no broader privileges than required.
- [x] **Privileged operations identified?**
  - [x] Which operations require elevated privileges? Creating and managing queue directories, mapping large files and adjusting file-system or JVM limits may require elevated privileges depending on the environment.
  - [x] How are they protected? These operations are normally performed by deployment tooling or administrators; `security-review.adoc` highlights these considerations so operators can configure least-privilege roles appropriately.

### Cryptographic Controls (ISO 27001 Control A.8.24)
- [x] **Cryptography usage identified?**
  - [x] Is encryption used? Chronicle-Queue itself does not encrypt queue files; encryption at rest is usually provided by the underlying file system or by encrypting payloads at the application level, as documented in `security-review.adoc`.
  - [x] Is hashing used? Cryptographic hashing of queue contents is not a primary concern at this layer; any future checksumming or integrity mechanisms for queue files will be documented explicitly in `security-review.adoc`.
  - [x] Is TLS/SSL used? TLS applies at the transport layer (for example Chronicle-Network), not within Chronicle-Queue; this is clarified in the threat model so that operators do not rely on queues for transport encryption.
- [x] **Key management?**
  - [x] How are cryptographic keys managed? Where applications store encrypted payloads in queues, key management is the responsibility of those applications and external key-management systems, not Chronicle-Queue.
  - [x] Are keys hardcoded? Chronicle-Queue must not embed keys in code or configuration; using queues to store raw keys is discouraged in documentation.

### Network Security (ISO 27001 Control A.8.22)
- [x] **Network communication security?** This module itself does not open network connections; Chronicle-Network and service-layer modules are responsible for transport-level security.
  - [x] Does this module communicate over network? [N]
  - [x] If yes, is communication encrypted?
  - [x] How are network endpoints authenticated?
- [x] **Network configuration?**
  - [x] Secure defaults configured? Network configuration is out of scope for this module and handled by Chronicle-Network and surrounding services.
  - [x] Insecure protocols disabled? Network protocol choices are defined at the edge; Chronicle-Queue does not introduce additional network protocols.

### Vulnerability Management (ISO 27001 Control A.8.8)
- [ ] **Known vulnerabilities?**
  - [ ] Any open security issues in GitHub? Chronicle-Queue maintainers should periodically review open issues for data-loss or corruption bugs and record any security-relevant items in `security-review.adoc`.
  - [ ] Any CVEs against dependencies? Dependency scanning tools should be used to track CVEs in libraries used by Chronicle-Queue (for example memory-mapping utilities) and to drive updates.
- [ ] **Security testing?**
  - [ ] Fuzz testing performed? Targeted fuzzing of queue headers and index structures could help catch parsing weaknesses; this remains future work.
  - [ ] Security-specific test cases? Regression tests around corrupt data, roll failures and disk-full scenarios should be linked from `security-review.adoc` as security-relevant tests.
  - [ ] Penetration testing performed? Queue-specific penetration testing is typically carried out as part of a wider system; any findings that implicate Chronicle-Queue should be summarised in the module’s security review.

### Security Documentation
- [x] **Security review documented?**
  - [x] Location: `src/main/docs/security-review.adoc`
  - [x] Threat model documented?
  - [x] Security controls described?
  - [x] Known limitations documented?

## Improvement Tasks (ARCH_TODO Stage 5.5)

**Feeds into:** Improve Existing Module Documentation

### High Priority
- [x] Create missing architecture-overview.adoc (if needed)
- [x] Add missing front-matter to existing docs
- [ ] Fix broken cross-references
- [ ] Add `:sectnums:` where appropriate

### Medium Priority
- [ ] Expand brief architecture docs (if < 75 lines)
- [ ] Add "Trade-offs and Alternatives" section (following Chronicle-Bytes pattern)
- [ ] Add performance characteristics section
- [ ] Create decision log entries for undocumented decisions

### Low Priority
- [ ] Add diagrams (PlantUML or draw.io)
- [ ] Create example code snippets
- [ ] Expand requirements documentation
- [ ] Add cross-references to other module docs

## Code Quality Tasks

**Reference:** [../QUALITY_PLAYBOOK.md](../QUALITY_PLAYBOOK.md)

- [x] Run Checkstyle scan and document violations
  - Latest command: `mvn -q -pl Chronicle-Queue -am clean verify` on Java 21 (see `verify-chronicle-queue-java21.log`); Checkstyle reports `You have 0 Checkstyle violations.` for this module.
- [x] Run SpotBugs scan and document issues
  - The same build run executes SpotBugs with the shared quality profile; `BugInstance size is 0` and `Error size is 0`, so there are no outstanding SpotBugs findings for Chronicle-Queue under the current ruleset.
- [x] Identify any code review follow-ups from CODE_REVIEW_STATUS.md
  - No Chronicle-Queue-specific follow-ups are currently listed in `CODE_REVIEW_STATUS.md`; future review items will be tracked there and, where applicable, added to this TODO file.

## Notes

- 2025-11-18: Checkstyle and SpotBugs now run clean for Chronicle-Queue under the shared quality profile (see `verify-chronicle-queue-java21.log`). Remaining open TODO items (documentation improvements, checklists and completion items) are considered longer-running follow-ups and are tracked as deferred work in `TODO_STATUS.md`.

## Completion Checklist

Before marking this repository's contribution to ARCH_TODO as complete:

- [x] All "Module Information" sections filled out
- [x] Existing documentation audited
- [x] Requirements identified for ARCH_TODO Stage 1.75
- [x] Decisions identified for ARCH_TODO Stage 1.85
- [x] Glossary terms identified for ARCH_TODO Stage 1.5
- [x] Documentation gaps documented
- [ ] Improvement tasks prioritized
- [ ] Information contributed to relevant ARCH_TODO stages

---

**When complete, update:** [../ARCH_TODO.md](../ARCH_TODO.md) Stage 3 tracking matrix
