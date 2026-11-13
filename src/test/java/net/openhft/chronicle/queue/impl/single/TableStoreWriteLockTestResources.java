/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.TestProcessCleanup;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

final class TableStoreWriteLockTestResources implements AutoCloseable {
    private final List<OwnedResource> owned = new ArrayList<>();
    private final List<String> diagnostics = new ArrayList<>();
    private boolean closed;

    <T extends AutoCloseable> T own(T resource, String description) {
        register(resource, description);
        return resource;
    }

    Process ownProcess(Process process, String description) {
        register(() -> {
            addDiagnostic(description + ": aliveBefore=" + process.isAlive()
                    + ", stdout=" + availableOutput(process.getInputStream())
                    + ", stderr=" + availableOutput(process.getErrorStream()));
            try {
                stopProcess(process, 2, TimeUnit.SECONDS);
            } finally {
                addDiagnostic(description + ": aliveAfter=" + process.isAlive());
            }
        }, description);
        return process;
    }

    ExecutorService ownExecutor(ExecutorService executor) {
        register(() -> stopExecutor(executor), "lock acquisition executor");
        return executor;
    }

    private void register(AutoCloseable resource, String description) {
        synchronized (this) {
            if (!closed) {
                owned.add(new OwnedResource(resource, description));
                return;
            }
        }
        // A timed-out body can finish launching after teardown has taken its snapshot.
        // A late owner must be released, never silently added to an already closed fixture.
        IllegalStateException failure = new IllegalStateException("Fixture already closed: " + description);
        try {
            resource.close();
        } catch (Throwable cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
        }
        throw failure;
    }

    @Override
    public void close() {
        List<OwnedResource> snapshot;
        synchronized (this) {
            if (closed)
                return;
            closed = true;
            snapshot = new ArrayList<>(owned);
            owned.clear();
        }
        Collections.reverse(snapshot);
        AssertionError failure = null;
        boolean interrupted = Thread.currentThread().isInterrupted();
        try {
            for (OwnedResource owner : snapshot) {
                long started = System.nanoTime();
                try {
                    owner.resource.close();
                    addDiagnostic(owner.description + ": closed in "
                            + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) + " ms");
                } catch (Throwable cause) {
                    AssertionError next = new AssertionError("Could not clean " + owner.description, cause);
                    if (failure == null)
                        failure = next;
                    else
                        failure.addSuppressed(next);
                    addDiagnostic(owner.description + ": cleanup failed: " + cause);
                } finally {
                    interrupted |= Thread.currentThread().isInterrupted();
                }
            }
        } finally {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
        if (failure != null)
            throw failure;
    }

    synchronized String diagnostics() {
        return String.join("\n", diagnostics);
    }

    private synchronized void addDiagnostic(String message) {
        diagnostics.add(message);
    }

    static void stopProcess(Process process, long timeout, TimeUnit unit) {
        TestProcessCleanup.stop(process, timeout, unit);
    }

    static void stopExecutor(ExecutorService executor) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        boolean interrupted = Thread.interrupted();
        try {
            executor.shutdownNow();
            while (!executor.isTerminated()) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0)
                    throw new AssertionError("Lock acquisition executor did not terminate: " + executor);
                try {
                    executor.awaitTermination(remaining, TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    static void deleteDirectory(Path directory, long timeout, TimeUnit unit) {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        boolean interrupted = Thread.interrupted();
        try {
            do {
                // Release can cascade through the lock, its bound value and mapped bytes.
                // Keep checking this fixture's actual deletion outcome under fork contention.
                BackgroundResourceReleaser.releasePendingResources();
                if (!directory.toFile().exists() || IOTools.deleteDirWithFiles(directory.toFile()))
                    return;
                if (System.nanoTime() >= deadline)
                    throw new AssertionError("Could not delete lock fixture " + directory
                            + " after " + unit.toMillis(timeout) + " ms; remaining="
                            + java.util.Arrays.toString(directory.toFile().list()));
                // Deletion is the observed condition; deferred unmapping supplies no completion latch.
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
                interrupted |= Thread.interrupted();
            } while (true);
        } finally {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    private static String availableOutput(InputStream stream) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int available;
            while (bytes.size() < 4096 && (available = stream.available()) > 0) {
                int read = stream.read(buffer, 0, Math.min(Math.min(available, buffer.length), 4096 - bytes.size()));
                if (read <= 0)
                    break;
                bytes.write(buffer, 0, read);
            }
            return bytes.toString();
        } catch (IOException e) {
            return "unavailable: " + e;
        }
    }

    private static final class OwnedResource {
        final AutoCloseable resource;
        final String description;

        OwnedResource(AutoCloseable resource, String description) {
            this.resource = resource;
            this.description = description;
        }
    }
}
