# Chronicle Queue – In-flight Test Research

This note consolidates the latest research inputs so test writers can encode the right invariants before modifying `TESTS_TODO.md`. Use it as a scratchpad and keep it close to the sources referenced below.

## LogAppender Builder Semantics
- **Why it matters:** Chronicle-Logger adapters expose `blockSize`, `bufferCapacity`, `rollCycle`, and `wireType` overrides which should behave identically inside Chronicle Queue. Misconfigurations often surface when users reopen the same queue path across JVMs.
- **Sources:** Chronicle-Logger README and the binding-specific tests demonstrate the supported knob set and lifecycle (`Chronicle-Logger/logger-core/src/main/java/net/openhft/chronicle/logger/LogAppenderConfig.java`). Stack Overflow threads highlight compatibility pitfalls. [[1]](#1), [[2]](#2)
- **Candidate Assertions:** Parameterise `SingleChronicleQueueBuilderTest`/`RollCycleDefaultingTest` to cover each knob combination, reopen queues after heavy writes, and assert `queue.rollCycle()` plus `createTailer().sourceId()` remain stable. Add smoke tests that mimic Logback/Log4J configs to ensure log replay works end-to-end.

## Async Buffer & Ring Bridge Behaviour
- **Why it matters:** Enterprise builds rely on `BufferMode.Asynchronous` (writer and reader) to add a ring buffer that absorbs OS jitter; ordering and latency guarantees must match the documentation.
- **Sources:** Queue Enterprise docs/javadoc and the ring-buffer blog outline the behaviour and expectations for the asynchronous modes. [[3]](#3), [[4]](#4)
- **Candidate Assertions:** Extend `ChronicleQueueTwoThreadsTest` with saturation scenarios that compare synchronous vs asynchronous modes, assert ordering under backlog, and capture percentile latencies similar to the Enterprise benchmarks.

## File Growth, Truncation, and Zero-length Payloads
- **Why it matters:** Replication backfill, preloading, and CLI consumers must treat incomplete frames and empty payloads as “not present” without corrupting indices.
- **Sources:** Enterprise replication notes and Pretoucher docs explain how incomplete SPB frames appear during backfill, and Chronicle-Queue-Demo shows truncation/restart flows. [[4]](#4), [[5]](#5)
- **Candidate Assertions:** Enhance `ReaderResizesFileTest`/`DeleteFileTest` by crafting partial SPB frames with Wires helpers, keeping tailers mid-document through roll cycles, and verifying zero-length payloads are skipped without advancing `tailer.index()`.

## Named Tailer Start Strategies (QueueOffsetSpec)
- **Why it matters:** Failover scenarios depend on Named Tailers persisting exact indexes and respecting `QueueOffsetSpec` expressions (start, end, snapshot, roll-time).
- **Sources:** `QueueOffsetSpec` javadoc plus Enterprise documentation on failover/replay. [[6]](#6)
- **Candidate Assertions:** Persist a named tailer after writing N messages, restart the fixture, and assert resume-at-index semantics for `toStart()`, `toEnd()`, and specific roll-time offsets. Include DST-sensitive specs and invalid tokens to ensure parser failures bubble up.

## CLI / Text-Wire Consumers
- **Why it matters:** Users copy the demo mains verbatim; tests should reflect their expectations (tailer back-off, `MessageHistory`, MethodWriter bridging).
- **Sources:** Chronicle-Queue-Demo’s `simple-input`, `message-history-demo`, and CLI tests in this repo. [[7]](#7)
- **Candidate Assertions:** Script CLI-style tests that feed stdin, read stdout, assert `Jvm.pause` is honoured when queues are empty, and verify `MessageHistory` increments on each hop through a `MethodReader` bridge.

## Pretouch & Page-size Constraints
- **Why it matters:** Pretoucher threads keep appenders ahead of page faults; misuse after shutdown or on unsupported filesystems should fail fast.
- **Sources:** Pretoucher/MicroToucher javadoc and Chronicle-Core/Bytes TODOs regarding page-size maths. [[5]](#5), [[8]](#8)
- **Candidate Assertions:** Start a pretoucher, stream writes across roll boundaries, confirm no tailer stalls, then `shutdown()` and ensure subsequent `execute()` calls throw. Add platform-aware skips when `OS.isWindows()` or tiny page sizes would make the expectation invalid.

## Service-driven Replication & Failover Realism
- **Why it matters:** Enterprise guidance promises acknowledged replication with deterministic leader/follower transitions. Core queue tests should encode at least minimal ordering/lag expectations for named tailers and start strategies.
- **Sources:** Chronicle replication articles and failover docs. [[9]](#9)
- **Candidate Assertions:** Simulate a three-node setup (can be lightweight) where heartbeats lapse, followers take over, and catch-up ordering is asserted. Validate no duplicate publications and bounded lag during reconnect.

## Roll-cycle Mechanics & Buffer Mode Interactions
- **Why it matters:** EOF markers and `toEnd()` semantics differ at roll boundaries; asynchronous write buffers can delay visibility if not exercised.
- **Sources:** Queue docs/javadoc plus async-mode research. [[3]](#3), [[4]](#4)
- **Candidate Assertions:** Write through a roll boundary, ensure tailers hop cycles deterministically, and re-run under asynchronous write mode to confirm visibility behaviour under backpressure.

## Ring Buffer Internals (Enterprise Ring / Queue Zero)
- **Why it matters:** Enterprise ring targets 99.99th percentile jitter reduction; tests should prove bounded latency and clean lifecycle for SPSC usage.
- **Sources:** Chronicle Ring vs Disruptor article and RingZero docs. [[11]](#11)
- **Candidate Assertions:** Two-thread SPSC tests that stress the ring buffer, measure latency envelope, and confirm tailer shutdown/unsubscribe semantics.

## “Do Not Do This” Guardrails
- **Why it matters:** Users frequently attempt unsupported deployment patterns (e.g., NFS-backed queues, assuming TCP message boundaries). Tests and fixtures should surface these hazards early.
- **Sources:** Community guidance and Stack Overflow discussions. [[12]](#12)
- **Candidate Actions:** Add fixture helpers that detect network filesystems and skip with clear messaging, and craft malformed TCP/SPB frames to enforce byte-stream assumptions in readers.

---

### References
1. <https://github.com/OpenHFT/Chronicle-Logger>  
2. <https://stackoverflow.com/questions/55475347/log-level-set-to-trace-for-chronicle-logger-causes-abstractmethoderror>  
3. <https://javadoc.io/doc/net.openhft/chronicle-queue/5.19.73/net/openhft/chronicle/queue/impl/single/SingleChronicleQueueBuilder.html>  
4. <https://foojay.io/today/reducing-tail-latencies-with-chronicle-queue-enterprise/>  
5. <https://www.javadoc.io/static/net.openhft/chronicle-queue/5.19.40/net/openhft/chronicle/queue/impl/single/Pretoucher.html>  
6. <https://javadoc.io/static/net.openhft/chronicle-queue/5.19.2/net/openhft/chronicle/queue/QueueOffsetSpec.html>  
7. <https://github.com/OpenHFT/Chronicle-Queue-Demo>  
8. <https://www.javadoc.io/doc/net.openhft/chronicle-queue/5.24ea19/net/openhft/chronicle/queue/impl/single/Pretoucher.html>  
9. <https://chronicle.software/acknowledged-replication-of-queue-across-the-network/>  
10. <https://github.com/OpenHFT/Chronicle-Queue>  
11. <https://chronicle.software/chronicle-ring-vs-lmax-disruptor/>  
12. <https://stackoverflow.com/questions/68854354/how-to-share-chronicle-queue-between-multiple-micro-services-in-aws>
