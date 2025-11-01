/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.main;

import net.openhft.chronicle.queue.internal.main.InternalBenchmarkMain;

/**
 * BenchmarkMain is an entry point for running benchmarking tools for Chronicle Queue.
 * <p>
 * This class makes use of several system properties for configuration:
 * <ul>
 *   <li><b>throughput</b>: Specifies the throughput in MB/s (default: 250)</li>
 *   <li><b>runtime</b>: Specifies the runtime duration in seconds (default: 300)</li>
 *   <li><b>path</b>: Specifies the base path for the benchmark (default: OS temporary directory)</li>
 * </ul>
 * The system properties can be set using JVM arguments.
 */
public final class BenchmarkMain {

    /**
     * The main method that triggers the benchmarking process.
     * Delegates the execution to {@link InternalBenchmarkMain#main(String[])}.
     *
     * @param args Command-line arguments
     */
    public static void main(String[] args) {
        InternalBenchmarkMain.main(args);
    }
}
