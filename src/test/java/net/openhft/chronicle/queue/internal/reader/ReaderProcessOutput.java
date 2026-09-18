/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.reader;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Owns both output pipes and the child, including a timed-out test's cleanup. */
final class ReaderProcessOutput {
    private ReaderProcessOutput() {
    }

    static String readAndClose(Process process) throws Exception {
        ExecutorService readers = Executors.newFixedThreadPool(2, runnable ->
                new Thread(runnable, "chronicle-reader-output"));
        Throwable failure = null;
        try {
            // Waiting for exit before consuming output can fill either pipe and deadlock
            // the child. Keep stdout separate so timestamp assertions remain unchanged.
            Future<String> stdout = readers.submit(() -> drain(process.getInputStream()));
            Future<String> stderr = readers.submit(() -> drain(process.getErrorStream()));
            int exit = process.waitFor(); // The enclosing test retains its original deadline.
            assertEquals("Reader stderr: " + stderr.get(2, TimeUnit.SECONDS), 0, exit);
            return stdout.get(2, TimeUnit.SECONDS);
        } catch (Exception | Error thrown) {
            failure = thrown;
            if (thrown instanceof InterruptedException)
                Thread.currentThread().interrupt();
            throw thrown;
        } finally {
            try {
                close(process, readers);
            } catch (Exception | Error cleanupFailure) {
                if (failure == null)
                    throw cleanupFailure;
                failure.addSuppressed(cleanupFailure);
            }
        }
    }

    private static String drain(InputStream stream) throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int read;
        while ((read = stream.read(buffer)) != -1) {
            // Continue draining noisy startup diagnostics without unbounded heap growth.
            int keep = Math.min(read, 2 * 1024 * 1024 - captured.size());
            if (keep > 0)
                captured.write(buffer, 0, keep);
        }
        return new String(captured.toByteArray(), StandardCharsets.UTF_8);
    }

    private static void close(Process process, ExecutorService readers) throws Exception {
        boolean interrupted = Thread.interrupted();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        try {
            if (process.isAlive())
                process.destroyForcibly();
            while (process.isAlive() && System.nanoTime() < deadline) {
                try {
                    process.waitFor(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
            assertFalse("Reader child remains alive after forced termination: " + process, process.isAlive());
        } finally {
            try {
                readers.shutdownNow();
                java.io.IOException streamFailure = null;
                for (java.io.Closeable stream : new java.io.Closeable[]{
                        process.getInputStream(), process.getErrorStream(), process.getOutputStream()}) {
                    try {
                        stream.close();
                    } catch (java.io.IOException thrown) {
                        if (streamFailure == null)
                            streamFailure = thrown;
                        else
                            streamFailure.addSuppressed(thrown);
                    }
                }
                while (!readers.isTerminated() && System.nanoTime() < deadline) {
                    try {
                        readers.awaitTermination(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                    } catch (InterruptedException ignored) {
                        interrupted = true;
                    }
                }
                assertTrue("Reader output workers remain alive", readers.isTerminated());
                if (streamFailure != null)
                    throw streamFailure;
            } finally {
                if (interrupted)
                    Thread.currentThread().interrupt();
            }
        }
    }
}
