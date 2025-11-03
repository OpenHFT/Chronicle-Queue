/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.bench;

public class BenchmarkUtils {

    /**
     * {@link Thread#join()} a thread and deal with interrupted exception
     *
     * @param t The thread to join
     */
    public static void join(Thread t) {
        try {
            t.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
