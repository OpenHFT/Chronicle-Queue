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

import static org.junit.Assert.assertEquals;

/** Owns a reader subprocess until both its output and its termination are accounted for. */
final class ReaderProcessOutput {
    private ReaderProcessOutput() {
    }

    @SuppressWarnings("try") // The stdin and termination resources exist to guarantee cleanup on every exit path.
    static String read(Process process, long timeout, TimeUnit unit) throws IOException, InterruptedException {
        final long deadline = System.nanoTime() + unit.toNanos(timeout);
        final ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        final ByteArrayOutputStream stderr = new ByteArrayOutputStream();
        final byte[] buffer = new byte[8192];
        try (OutputStream in = process.getOutputStream();
             InputStream out = process.getInputStream();
             InputStream err = process.getErrorStream();
             Closeable termination = () -> terminate(process)) {
            while (process.isAlive()) {
                // Drain both pipes while the child runs: either full pipe can prevent its exit.
                readAvailable(out, stdout, buffer);
                readAvailable(err, stderr, buffer);
                final long remaining = deadline - System.nanoTime();
                if (remaining <= 0)
                    throw new AssertionError("Reader subprocess did not exit within " + unit.toMillis(timeout)
                            + " ms; stdout: " + stdout + "; stderr: " + stderr);
                process.waitFor(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(10)), TimeUnit.NANOSECONDS);
            }
            stdout.write(IOTools.readAsBytes(out));
            stderr.write(IOTools.readAsBytes(err));
            assertEquals("Reader subprocess failed; stdout: " + stdout + "; stderr: " + stderr,
                    0, process.exitValue());
            return stdout.toString();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    private static void readAvailable(InputStream input, ByteArrayOutputStream output, byte[] buffer) throws IOException {
        int available = input.available();
        if (available > 0) {
            int count = input.read(buffer, 0, Math.min(available, buffer.length));
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
                long remaining = deadline - System.nanoTime();
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
