# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Chronicle Queue is an ultra-low-latency, persisted messaging library for Java. It provides broker-less, off-heap storage using memory-mapped files, enabling millions of messages per second with microsecond latencies while avoiding GC pauses.

## Build and Test Commands

```bash
# Full verification (preferred)
mkdir -p logs
mvn verify -l logs/mvn-verify.log

# Run a specific test
mvn -Dtest=TestClassName test -l logs/mvn-test.log

# Fast compile without tests
mvn -DskipTests package -l logs/mvn-skip-tests.log

# Run benchmarks (only when explicitly requested)
mvn -P run-benchmarks test -l logs/mvn-benchmarks.log

# Enable zero-cost assertions
mvn -P assertions test -l logs/mvn-assertions.log

# Review warnings/errors in logs
rg -n '^\[(WARNING|ERROR)\]|SLF4J\(W\)|\bWARNING:|\bwarning:' logs/mvn-verify.log
```

## Architecture

### Core Abstractions

- **ChronicleQueue** (`net.openhft.chronicle.queue.ChronicleQueue`): Main entry point. Factory methods create queues backed by directories of `.cq4` files.
- **ExcerptAppender**: Write-only handle for appending messages. Thread-local via `queue.acquireAppender()`.
- **ExcerptTailer**: Read handle for consuming messages. Created via `queue.createTailer()` or `queue.createTailer("name")` for restartable/named tailers.
- **DocumentContext**: Low-level read/write context from `appender.writingDocument()` or `tailer.readingDocument()`.

### Key Implementation Classes

- **SingleChronicleQueue** (`impl/single/SingleChronicleQueue.java`): The primary queue implementation supporting roll cycles.
- **SingleChronicleQueueBuilder**: Fluent builder for queue configuration (roll cycle, wire type, block size, etc.).
- **StoreAppender** / **StoreTailer**: Internal implementations of appender/tailer for file-backed storage.
- **TableStore** / **SingleTableStore**: Manages queue metadata in `metadata.cq4t` files.

### File Structure

- Queue data: `{queue-dir}/{cycle}.cq4` (e.g., `20241231.cq4` for daily roll)
- Metadata: `{queue-dir}/metadata.cq4t`
- Roll cycles determine file naming and capacity (see `RollCycles` enum)

### Package Organization

- `net.openhft.chronicle.queue` - Public API interfaces
- `net.openhft.chronicle.queue.impl.single` - SingleChronicleQueue implementation
- `net.openhft.chronicle.queue.impl.table` - Table store for metadata/locks
- `net.openhft.chronicle.queue.main` - CLI entry points (DumpMain, ReaderMain, etc.)
- `net.openhft.chronicle.queue.internal` - Internal implementation details (not public API)

### Dependencies

Chronicle Queue builds on other OpenHFT libraries:
- **chronicle-core**: Low-level utilities, memory access, JVM interaction
- **chronicle-bytes**: Off-heap byte buffers and memory-mapped file access
- **chronicle-wire**: Serialization framework (Binary, Text, JSON)
- **chronicle-threads**: Threading utilities, event loops, pausers

## CLI Tools

Scripts in `bin/` require the uber-jar (`mvn package` first):

```bash
./bin/dump_queue.sh <queue-dir>           # Dump queue as text
./bin/queue_reader.sh -d <dir> [options]  # Read/tail queue
./bin/queue_writer.sh -d <dir> -m <method> <files>  # Write YAML to queue
./bin/unlock_queue.sh <queue-dir>         # Force unlock stuck queue
```

## Constraints

- **Java 8 baseline**: Avoid newer language features.
- **ISO-8859-1 encoding**: Source files must stay in code points 0-255.
- **Binary compatibility**: Public APIs, wire formats, and on-disk layouts must be preserved unless explicitly changing.
- **Performance-critical**: Avoid allocations and synchronization on hot paths.
- **No network filesystems**: Chronicle Queue only works on local filesystems, not NFS/SAN.

## Testing Notes

- Tests use JUnit 5 (`junit-jupiter`) and JUnit 4 via vintage engine
- Test base classes in `net.openhft.chronicle.queue` often extend `QueueTestCommon`
- Many tests use `@TempDir` or create temp directories for queue storage
- Stress tests in `impl/single/stress/` package

## Key System Properties

See `docs/systemProperties.adoc` for full list. Notable ones:
- `chronicle.queue.checkrollcycle` - Warn on roll cycle file creation
- `chronicle.queue.warnSlowAppenderMs` - Slow appender threshold (default 100ms)
- `chronicle.table.store.timeoutMS` - Table store lock timeout (default 10s)
