/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
