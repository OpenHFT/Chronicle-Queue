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

import net.openhft.chronicle.queue.internal.main.InternalPingPongMain;

/**
 * PingPongMain is an entry point for running a ping-pong style benchmark using Chronicle Queue.
 * <p>
 * This class uses the following system properties for configuration:
 * <ul>
 *   <li><b>runtime</b>: Specifies the duration of the benchmark in seconds (default: 30)</li>
 *   <li><b>path</b>: Specifies the base path for the benchmark files (default: OS temporary directory)</li>
 * </ul>
 * These properties can be set as JVM arguments.
 */
public final class PingPongMain {

    /**
     * The main method that triggers the ping-pong benchmark.
     * Delegates execution to {@link InternalPingPongMain#main(String[])}.
     *
     * @param args Command-line arguments
     */
    public static void main(String[] args) {
        InternalPingPongMain.main(args);
    }
}
