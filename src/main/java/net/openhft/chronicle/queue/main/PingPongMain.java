/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
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
