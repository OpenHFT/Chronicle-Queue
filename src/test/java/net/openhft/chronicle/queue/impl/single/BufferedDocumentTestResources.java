/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/** Owns this fixture's worker and closes its storage only after the worker exits. */
final class BufferedDocumentTestResources implements AutoCloseable {
    private final ExecutorService worker;
    private final long timeoutNanos;
    private final List<AutoCloseable> storage = new ArrayList<>();
    private boolean closed;

    BufferedDocumentTestResources(ExecutorService worker) {
        this(worker, 5, TimeUnit.SECONDS);
    }

    BufferedDocumentTestResources(ExecutorService worker, long timeout, TimeUnit unit) {
        this.worker = worker;
        timeoutNanos = unit.toNanos(timeout);
    }

    synchronized <T extends AutoCloseable> T own(T resource) {
        if (closed)
            throw new IllegalStateException("Fixture already closed");
        storage.add(resource);
        return resource;
    }

    @Override
    public void close() throws Exception {
        if (closed)
            return;
        boolean interrupted = Thread.interrupted();
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            worker.shutdownNow();
            while (!worker.isTerminated()) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0)
                    throw new AssertionError("Buffered worker remains alive; storage retained");
                try {
                    worker.awaitTermination(remaining, TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            // Worker termination also publishes resources created after a timed-out Future.get.
            Throwable failure = null;
            synchronized (this) {
                closed = true;
                for (int i = storage.size() - 1; i >= 0; i--) {
                    try {
                        storage.get(i).close();
                    } catch (Exception | Error e) {
                        if (failure == null)
                            failure = e;
                        else if (failure != e)
                            failure.addSuppressed(e);
                    }
                }
                storage.clear();
            }
            if (failure instanceof Error)
                throw (Error) failure;
            if (failure != null)
                throw (Exception) failure;
        } finally {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }
}
