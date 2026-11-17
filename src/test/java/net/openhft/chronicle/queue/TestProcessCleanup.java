/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.io.Closeable;

import java.util.concurrent.TimeUnit;

/** Owns a test subprocess until its bounded termination has been observed. */
public final class TestProcessCleanup implements AutoCloseable {
    private final Process process;
    private final long timeout;
    private final TimeUnit unit;

    public TestProcessCleanup(Process process, long timeout, TimeUnit unit) {
        this.process = process;
        this.timeout = timeout;
        this.unit = unit;
    }

    public Process process() {
        return process;
    }

    @Override
    public void close() {
        try {
            stop(process, timeout, unit);
        } finally {
            Closeable.closeQuietly(process.getInputStream(), process.getErrorStream(), process.getOutputStream());
        }
    }

    public static void stop(Process process, long timeout, TimeUnit unit) {
        final long started = System.nanoTime();
        final long deadline = started + unit.toNanos(timeout);
        boolean interrupted = Thread.interrupted();
        try {
            process.destroy();
            final long destroyCompleted = System.nanoTime();
            boolean forced = interrupted;
            long forcedAt = -1;
            if (forced) {
                forcedAt = System.nanoTime();
                process.destroyForcibly();
            }
            while (process.isAlive()) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    if (!forced) {
                        forcedAt = System.nanoTime();
                        process.destroyForcibly();
                    }
                    throw new AssertionError("Locking subprocess remains alive after termination deadline ("
                            + unit.toMillis(timeout) + " ms); elapsedMillis="
                            + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)
                            + ", destroyRequestMillis=" + TimeUnit.NANOSECONDS.toMillis(destroyCompleted - started)
                            + ", forcedAfterMillis=" + TimeUnit.NANOSECONDS.toMillis(forcedAt - started)
                            + ", process=" + process);
                }
                try {
                    if (process.waitFor(forced ? remaining : remaining / 2, TimeUnit.NANOSECONDS))
                        return;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
                if (!forced) {
                    forcedAt = System.nanoTime();
                    process.destroyForcibly();
                    forced = true;
                }
            }
        } finally {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

}
