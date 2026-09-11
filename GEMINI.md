# Chronicle Queue Analysis (`GEMINI.md`)

This document provides a comprehensive overview of the Chronicle Queue project, intended as a guide for developers and contributors.

## Project Overview

Chronicle Queue is a high-performance, low-latency, broker-less messaging library for Java. It is designed for applications that require durable, persisted messaging with millions of events per second. The core of Chronicle Queue is its off-heap storage model, which minimizes Garbage Collection (GC) pauses and allows for predictable, low-latency performance.

### Key Features:

*   **Ultra-Low Latency:** Achieves sub-microsecond latencies for message passing between JVMs on the same machine.
*   **High Throughput:** Supports millions of messages per second on a single thread.
*   **Persistence:** All messages are persisted to memory-mapped files, ensuring durability and allowing for replay.
*   **Broker-less Architecture:** Reduces complexity and eliminates a single point of failure.
*   **Append-Only Log Structure:** Messages are stored in append-only files, organized by a configurable roll cycle (e.g., daily, hourly).
*   **Concurrent Access:** Supports a single writer and multiple, independent readers (tailers) per queue.
*   **Flexible Serialization:** Integrates with Chronicle Wire to support various serialization formats, including binary, text, YAML, and JSON.

### Architecture:

Chronicle Queue's architecture is built on a layered stack of Chronicle libraries:

*   **Chronicle-Core:** Provides fundamental utilities and lifecycle management.
*   **Chronicle-Bytes:** Manages off-heap and memory-mapped storage.
*   **Chronicle-Wire:** Handles message serialization and deserialization.
*   **Chronicle-Threads:** Provides threading and event loop utilities.

The core data structure is a directory of append-only roll files (`.cq4`), which contain message payloads and tiered index metadata. This design enables efficient sequential and random access to messages.

## Building and Running

Chronicle Queue is a standard Maven project. The following commands can be used to build, test, and install the project.

### Building the Project

To build the project and run the unit tests, use the following command:

```bash
mvn clean install
```

This will compile the source code, run the tests, and install the resulting JAR files into your local Maven repository.

### Running Benchmarks

The project includes a suite of benchmarks to measure performance. To run the benchmarks, you can use the `run-benchmarks` Maven profile:

```bash
mvn clean install -Prun-benchmarks
```

### Running Command-Line Tools

Chronicle Queue provides command-line tools for inspecting and manipulating queues. For example, to dump the contents of a queue to the console, you can use the `DumpMain` class:

```bash
mvn exec:java -Dexec.mainClass="net.openhft.chronicle.queue.main.DumpMain" -Dexec.args="<queue-directory>"
```

## Development Conventions

*   **Coding Style:** The project follows standard Java coding conventions.
*   **Testing:** The project has a comprehensive test suite, including unit, integration, and performance tests. New features and bug fixes should be accompanied by corresponding tests.
*   **Dependencies:** The project uses a bill of materials (BOM) to manage dependency versions.
*   **Concurrency:** The project has a strict concurrency model. `ExcerptAppender` instances are single-threaded, while `ExcerptTailer` instances are not thread-safe.
*   **Documentation:** The project has extensive documentation in the `docs` and `src/main/docs` directories, written in AsciiDoc.
