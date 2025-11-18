# Chronicle-Queue - Repository TODO

**📋 Part of:** [Chronicle Architecture Documentation](../ARCH_TODO.md)
**Module Layer:** Layer 1 (Data Structures & Serialization)
**Priority:** 🔴 P0
**Last Updated:** 2025-11-16

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
  - [ ] If no: Note as gap for ARCH_TODO Stage 5.5 - there is no `src/main/docs/architecture-overview.adoc`; high-level architecture is currently covered by `README.adoc` and `docs/How_it_works.adoc`.
- [x] Check if `src/main/docs/project-requirements.adoc` exists
  - [ ] If yes: Review for ARCH_TODO Stage 1.75 (Requirements Overview)
  - [x] If no: Note as gap for FUNC_TODO.md - no `project-requirements.adoc` exists for Chronicle-Queue; requirements need to be captured and given IDs (`QUEUE-FN-*`, `QUEUE-NF-*`).
- [x] Check if `src/main/docs/decision-log.adoc` exists
  - [x] If yes: Review for ARCH_TODO Stage 1.85 (Decision Log Overview) - `src/main/docs/decision-log.adoc` exists and documents key decisions (`CQ-FN-101`, `CQ-FN-102`, `CQ-NF-P-201`, `CQ-NF-S-301`, `CQ-NF-O-401`) with context, alternatives and links to `docs/`.
  - [ ] If no: Note as gap for DECISION_TODO.md
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
  - [ ] Requirements are testable and verifiable?
- [x] **Non-functional requirements documented?** Partially - performance, security and operability expectations are now captured in `QUEUE-NF-P-*`, `QUEUE-NF-S-*` and `QUEUE-NF-O-*` entries in `project-requirements.adoc`, with additional narrative in `README.adoc`, `docs/performance.adoc`, `docs/systemProperties.adoc` and decision-log entries.
  - [x] Performance requirements (QUEUE-NF-P-NNN)
  - [x] Security requirements (QUEUE-NF-S-NNN)
  - [x] Operability requirements (QUEUE-NF-O-NNN)

### Design Outputs (ISO 9001 Clause 8.3.5)
- [x] **Architecture documented?** Yes - `src/main/docs/architecture-overview.adoc` now provides the canonical overview, with `README.adoc` and `docs/How_it_works.adoc` as supporting material.
  - [x] Location: `src/main/docs/architecture-overview.adoc`
  - [x] Describes key components and their interactions?
  - [x] Includes interface specifications? (High-level API roles, queue file structure and builder configuration are described.)
- [ ] **APIs and interfaces specified?**
  - [ ] Public API documented (JavaDoc)?
  - [ ] Integration points with other modules described?

### Design Verification (ISO 9001 Clause 8.3.4)
- [ ] **Requirements traceable to tests?**
  - [ ] Test classes reference requirement IDs in comments/docs?
  - [ ] Coverage: What % of requirements have corresponding tests?
- [x] **Test strategy documented?**
  - [x] Unit test approach
  - [x] Integration test approach
  - [x] Performance test approach (if applicable)
- [ ] **Code review evidence?**
  - [ ] PR review process followed?
  - [ ] Review comments addressed?

### Design Changes (ISO 9001 Clause 8.3.4)
- [x] **Architectural decisions documented?**
  - [x] Location: `src/main/docs/decision-log.adoc`
  - [x] Decisions include context, alternatives, rationale? (See `CQ-FN-101`, `CQ-FN-102`, `CQ-NF-P-201`, `CQ-NF-S-301`, `CQ-NF-O-401`.)
  - [x] Impact of changes assessed? (Each record discusses operational and behavioural consequences.)
- [ ] **Change history maintained?**
  - [ ] Git commit messages describe rationale?
  - [ ] Breaking changes documented in release notes?

## ISO 27001 Information Security Considerations

**Reference:** [../ARCHITECTURE_RESEARCH_GUIDE.md](../ARCHITECTURE_RESEARCH_GUIDE.md) - Security Research Topics

### Secure Coding (ISO 27001 Control A.8.28)
- [ ] **Input validation implemented?**
  - [ ] Where are untrusted inputs received? [List entry points]
  - [ ] How are malformed inputs handled?
  - [ ] Size limits enforced?
- [ ] **Bounds checking implemented?**
  - [ ] Buffer overflow prevention mechanisms?
  - [ ] Array access validation?
  - [ ] Off-heap memory bounds checked?
- [ ] **Static analysis performed?**
  - [ ] Checkstyle violations reviewed?
  - [ ] SpotBugs security patterns checked?
  - [ ] Suppressions justified and documented?

### Access Control (ISO 27001 Control A.8.3)
- [ ] **Access restrictions implemented?**
  - [ ] Are there authentication/authorization mechanisms? [Y/N]
  - [ ] If yes, where and how are they implemented?
  - [ ] Principle of least privilege followed?
- [ ] **Privileged operations identified?**
  - [ ] Which operations require elevated privileges?
  - [ ] How are they protected?

### Cryptographic Controls (ISO 27001 Control A.8.24)
- [ ] **Cryptography usage identified?**
  - [ ] Is encryption used? [Y/N - where?]
  - [ ] Is hashing used? [Y/N - which algorithms?]
  - [ ] Is TLS/SSL used? [Y/N - configuration?]
- [ ] **Key management?**
  - [ ] How are cryptographic keys managed?
  - [ ] Are keys hardcoded? [Y/N - if yes, flag as risk]

### Network Security (ISO 27001 Control A.8.22)
- [x] **Network communication security?** This module itself does not open network connections; Chronicle-Network and service-layer modules are responsible for transport-level security.
  - [ ] Does this module communicate over network? [N]
  - [ ] If yes, is communication encrypted?
  - [ ] How are network endpoints authenticated?
- [ ] **Network configuration?**
  - [ ] Secure defaults configured?
  - [ ] Insecure protocols disabled?

### Vulnerability Management (ISO 27001 Control A.8.8)
- [ ] **Known vulnerabilities?**
  - [ ] Any open security issues in GitHub?
  - [ ] Any CVEs against dependencies?
- [ ] **Security testing?**
  - [ ] Fuzz testing performed?
  - [ ] Security-specific test cases?
  - [ ] Penetration testing performed?

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

- [ ] All "Module Information" sections filled out
- [ ] Existing documentation audited
- [ ] Requirements identified for ARCH_TODO Stage 1.75
- [ ] Decisions identified for ARCH_TODO Stage 1.85
- [ ] Glossary terms identified for ARCH_TODO Stage 1.5
- [ ] Documentation gaps documented
- [ ] Improvement tasks prioritized
- [ ] Information contributed to relevant ARCH_TODO stages

---

**When complete, update:** [../ARCH_TODO.md](../ARCH_TODO.md) Stage 3 tracking matrix
