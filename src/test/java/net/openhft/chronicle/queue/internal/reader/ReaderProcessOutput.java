/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.reader;

import net.openhft.chronicle.core.io.IOTools;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

/** Owns a reader subprocess until both its output and its termination are accounted for. */
final class ReaderProcessOutput {
    private ReaderProcessOutput() {
    }

    @SuppressWarnings("try") // The last resource terminates the child before its pipes are closed.
    static String read(Process process, String description, long timeout, TimeUnit unit)
            throws IOException, InterruptedException {
        final long started = System.nanoTime();
        final long timeoutNanos = unit.toNanos(timeout);
        final ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        final ByteArrayOutputStream stderr = new ByteArrayOutputStream();
        final byte[] buffer = new byte[8192];
        try (OutputStream in = process.getOutputStream();
             InputStream out = process.getInputStream();
             InputStream err = process.getErrorStream();
             Closeable termination = () -> terminate(process)) {
            while (process.isAlive()) {
                // Either full pipe can prevent exit, so drain both while the child runs.
                readAvailable(out, stdout, buffer);
                readAvailable(err, stderr, buffer);
                final long remaining = timeoutNanos - (System.nanoTime() - started);
                if (remaining <= 0)
                    throw new AssertionError(description + " did not exit within " + unit.toMillis(timeout)
                            + " ms" + diagnostics(started, stdout, stderr));
                process.waitFor(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(10)), TimeUnit.NANOSECONDS);
            }
            stdout.write(IOTools.readAsBytes(out));
            stderr.write(IOTools.readAsBytes(err));
            if (process.exitValue() != 0)
                throw new AssertionError(description + " exited with code " + process.exitValue()
                        + diagnostics(started, stdout, stderr));
            return stdout.toString();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            final InterruptedException failure = new InterruptedException(description + " interrupted"
                    + diagnostics(started, stdout, stderr));
            failure.initCause(e);
            throw failure;
        }
    }

    private static String diagnostics(long started, ByteArrayOutputStream stdout, ByteArrayOutputStream stderr) {
        return "; elapsed=" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)
                + " ms; stdout: " + stdout + "; stderr: " + stderr;
    }

    private static void readAvailable(InputStream input, ByteArrayOutputStream output, byte[] buffer) throws IOException {
        final int available = input.available();
        if (available > 0) {
            final int count = input.read(buffer, 0, Math.min(available, buffer.length));
            if (count > 0)
                output.write(buffer, 0, count);
        }
    }

    private static void terminate(Process process) throws IOException {
        boolean interrupted = Thread.interrupted();
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        try {
            if (process.isAlive())
                process.destroyForcibly();
            while (process.isAlive()) {
                final long remaining = deadline - System.nanoTime();
                if (remaining <= 0)
                    throw new IOException("Reader subprocess is still alive after forced termination");
                try {
                    process.waitFor(remaining, TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }
}
