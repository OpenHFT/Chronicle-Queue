# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

Chronicle Queue is a persisted low-latency messaging framework for high-performance Java applications. It provides broker-less, off-heap messaging with microsecond latencies and supports millions of events per second.

**Key architectural principles:**
- **Off-heap storage**: Uses memory-mapped files to avoid GC pressure
- **Zero-allocation design**: Performance-critical paths avoid object allocation
- **Single Writer Principle**: Core concurrency pattern - multiple concurrent readers, write lock for appenders
- **Flyweight pattern**: Objects act as views over underlying `Bytes` data

## Build Commands

### Basic Build
```bash
# Standard build with tests
mvn clean install

# Build without tests (faster)
mvn clean install -DskipTests

# Package (creates uber-jar for command-line tools)
mvn package
```

### Running Tests
```bash
# Run all tests
mvn test

# Run single test class
mvn test -Dtest=ClassName

# Run specific test method
mvn test -Dtest=ClassName#methodName
```

### Benchmarks
```bash
# Run all benchmarks
mvn test -Prun-benchmarks

# Run specific benchmark classes (via exec-maven-plugin)
mvn exec:java -Dexec.mainClass="net.openhft.chronicle.queue.main.BenchmarkMain"
```

### Command-Line Tools

After `mvn package`, use the shell scripts in `bin/`:

```bash
# Read queue contents
./bin/queue_reader.sh -d <queue-directory>

# Dump queue as text
./bin/dump_queue.sh <queue-file.cq4>

# Write to queue (MethodWriter)
./bin/queue_writer.sh -d <queue-directory> -m <method-name> <yaml-file>

# View message history
./bin/history_reader.sh -d <queue-directory>

# Unlock a queue
./bin/unlock_queue.sh <queue-directory>

# Refresh queue metadata
./bin/refresh_queue.sh <queue-directory>
```

## Code Architecture

### Core Abstractions

**Queue Creation:**
- `SingleChronicleQueue` - Main queue implementation
- `SingleChronicleQueueBuilder` - Builder for queue configuration
- Uses file-per-cycle model (configurable: DAILY, HOURLY, MINUTELY, etc.)

**Writing:**
- `ExcerptAppender` - Thread-local appender for writing messages
- `acquireAppender()` - Reuses thread-local appender (pooled)
- `DocumentContext` - Try-with-resources context for writing

**Reading:**
- `ExcerptTailer` - Reader for sequential/random access
- `createTailer()` - Creates unnamed tailer
- `createTailer(name)` - Creates restartable/named tailer (persists position)
- `MethodReader` - High-level API to convert messages to method calls

**File Structure:**
- Queue directory contains `.cq4` files (one per cycle)
- `metadata.cq4t` - Table store containing metadata and write lock
- Roll cycles determine file names (e.g., `20231119.cq4` for DAILY)

### Key Packages

```
net.openhft.chronicle.queue/
├── ChronicleQueue.java              # Main interface
├── ExcerptAppender.java             # Writer interface
├── ExcerptTailer.java               # Reader interface
├── RollCycles.java                  # File rolling configuration
├── impl/
│   └── single/                      # SingleChronicleQueue implementation
│       ├── SingleChronicleQueue.java
│       ├── SingleChronicleQueueBuilder.java
│       ├── StoreTailer.java         # Tailer implementation
│       ├── StoreAppender.java       # Appender implementation
│       ├── SCQIndexing.java         # Excerpt indexing
│       └── Pretoucher.java          # Pre-touches pages for low latency
├── main/                            # Command-line tools
│   ├── DumpMain.java
│   ├── ReaderMain.java              # ChronicleReaderMain
│   └── ChronicleWriterMain.java
└── reader/                          # Reader framework
```

### Critical Implementation Details

**Locking Model:**
- Write lock stored in `metadata.cq4t` table store (changed from v4)
- Appenders acquire lock per write via `TableStoreWriteLock`
- Tailers are read-only (no lazy indexing in v5)

**Indexing:**
- Primary index: cycle number (e.g., days since epoch) + sequence number
- Index format: `((long) cycle << 32) | sequenceNumber` for DAILY
- Secondary indexes at configurable spacing (`indexSpacing`, `indexCount`)
- Use `SCQIndexing` for index management

**Wire Formats:**
- Default: `BINARY_LIGHT` (self-describing binary)
- Also supports: `FIELDLESS_BINARY`, `DEFAULT_ZERO_BINARY`, `DELTA_BINARY`
- NOT supported: TEXT, JSON, CSV, RAW

**Resource Management:**
- Memory-mapped files are cached and released lazily
- Call `queue.close()` to release resources explicitly
- Tailers: `((StoreTailer)tailer).releaseResources()` for explicit cleanup
- Uses `WeakReferenceCleaner` for off-heap memory tracking

## Testing Patterns

### Resource Cleanup
All tests must verify off-heap resources are released:
```java
// From chronicle-test-framework
@Test
public void testExample() {
    expectException("optional expected exception message");
    try (ChronicleQueue queue = SingleChronicleQueueBuilder.single(tmpDir).build()) {
        // test code
    }
    // assertReferencesReleased() - inherited from test base class
}
```

### Common Test Utilities
- `QueueTestCommon` - Base class for queue tests
- `OS.getTarget()` - Gets temp directory for test files
- `IOTools.deleteDirWithFiles()` - Cleanup test directories

### Test Organization
- `src/test/java/net/openhft/chronicle/queue/impl/single/` - Core implementation tests
- `src/test/java/net/openhft/chronicle/queue/bench/` - JLBH benchmarks
- `src/test/java/net/openhft/chronicle/queue/jitter/` - Jitter measurement tests
- `src/test/java/net/openhft/chronicle/queue/issue/` - Regression tests for specific issues

## Common Development Tasks

### Running a Single Test
```bash
cd /home/peter/Second/Chronicle-Queue
mvn test -Dtest=SingleChronicleQueueTest#testAppendAndRead
```

### Dumping Queue Contents
```java
// Programmatically
System.out.println(queue.dump());

// Command line (after mvn package)
./bin/dump_queue.sh queue-dir/20231119.cq4
```

### Creating and Using a Queue
```java
// Create queue
try (ChronicleQueue queue = SingleChronicleQueueBuilder
        .single("queue-path")
        .rollCycle(RollCycles.DAILY)
        .build()) {

    // Write
    ExcerptAppender appender = queue.acquireAppender();
    try (DocumentContext dc = appender.writingDocument()) {
        dc.wire().write("msg").text("Hello World");
    }

    // Read
    ExcerptTailer tailer = queue.createTailer();
    try (DocumentContext dc = tailer.readingDocument()) {
        if (dc.isPresent()) {
            String msg = dc.wire().read("msg").text();
        }
    }
}
```

### Using Method Reader/Writer (High-level API)
```java
interface MyInterface {
    void onMessage(String text);
}

// Writing
MyInterface writer = appender.methodWriter(MyInterface.class);
writer.onMessage("Hello");

// Reading
MyInterface processor = msg -> System.out.println("Got: " + msg);
MethodReader reader = tailer.methodReader(processor);
while (reader.readOne()) {
    // Messages converted to method calls on processor
}
```

## Important Notes

1. **Network file systems not supported**: Chronicle Queue requires local file systems. Do not use NFS, AFS, or SAN storage.

2. **Package visibility**: Code in `internal/`, `impl/`, and `main/` packages is NOT public API and may change without notice.

3. **Roll cycle immutability**: Once a queue's roll cycle is set, it cannot be changed. Attempting to open with different roll cycle will log warning and use existing cycle.

4. **Interrupts**: Chronicle Queue code does not check for thread interrupts for performance reasons. Avoid using with code that generates interrupts, or use separate queue instances per thread.

5. **Index availability**: When using double-buffering, `DocumentContext.index()` throws `IndexNotAvailableException` until the context is closed.

6. **File handles**: Chronicle Queue caches file handles. Close flushes data to disk. Use `Pretoucher` to pre-touch pages for lowest latency.

## Key Configuration Parameters

- `rollCycle()` - File rotation frequency (DAILY, HOURLY, etc.)
- `blockSize()` - Memory mapping block size (default 64MB, should be 4x message size)
- `indexSpacing()` - Space between indexed excerpts (higher = faster writes, slower random reads)
- `indexCount()` - Index array size (max indexed entries = indexCount²)
- `readBufferMode()` / `writeBufferMode()` - Buffering strategy (None, Copy, Asynchronous)
- `sourceId()` - Enables high-resolution timing across messages
- `doubleBuffer()` - Enables double-buffering for contended writes

## Dependencies

Core Chronicle dependencies (from pom.xml):
- `chronicle-core` - Low-level utilities, resource management
- `chronicle-bytes` - Off-heap memory access
- `chronicle-wire` - Serialization framework
- `chronicle-threads` - Low-latency thread management
- `affinity` - Thread affinity (optional)

## Performance Considerations

- Default: 99%ile latency under 1μs for same-machine IPC
- Throughput: 5M+ messages/second (96-byte messages on i7-4790)
- Use `Pretoucher` for lowest latency outliers (Enterprise feature)
- Enable double-buffering only for large messages with high write contention
- Resource tracing (system.properties) should be disabled in production

## Documentation

- `README.adoc` - Comprehensive user guide
- `docs/FAQ.adoc` - Common questions
- `docs/How_it_works.adoc` - Implementation details
- `docs/utilities.adoc` - Utility documentation
- `src/main/docs/system-properties.adoc` - System property reference
