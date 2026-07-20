/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.metrics;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.internal.util.InternalTailerRetention;

import java.io.File;

/**
 * A {@link StoreFileListener} that removes rolled files in-process by named-tailer position:
 * when a roll file is released it is deleted only if its cycle is below the retention floor - that
 * is, older than the last {@code N} cycles <em>and</em> read past by every registered named tailer.
 *
 * <p>Contrast with the age-based {@link RetentionFileListener}: this one never deletes a roll a
 * named tailer still needs, even while that reader is stopped (its persisted index protects it),
 * so retention is bounded by free disk rather than by a time window.
 *
 * <p>The listener is set on the builder before the queue exists, so attach the queue after building:
 * <pre>{@code
 * TailerPositionRetentionFileListener retention = new TailerPositionRetentionFileListener(2);
 * SingleChronicleQueue queue = builder.storeFileListener(retention).build();
 * retention.queue(queue);
 * }</pre>
 *
 * <p>On each release it sweeps the rolls that have fallen below the floor (a file only becomes
 * removable once the head has advanced {@code keepLastCycles} beyond it, which is a later release
 * than its own). The sweep is wrapped so retention can never destabilise the queue. Files left
 * behind by a previous process are the job of the out-of-process {@code TailerRetentionMain}.
 */
public final class TailerPositionRetentionFileListener implements StoreFileListener {

    private final int keepLastCycles;
    private volatile SingleChronicleQueue queue;

    /**
     * @param keepLastCycles the minimum number of most-recent cycles always kept (>= 1)
     */
    public TailerPositionRetentionFileListener(int keepLastCycles) {
        if (keepLastCycles < 1)
            throw new IllegalArgumentException("keepLastCycles must be >= 1");
        this.keepLastCycles = keepLastCycles;
    }

    /**
     * Attaches the queue whose rolls this listener reaps. Call once, after the queue is built.
     *
     * @param queue the queue this listener is registered on
     */
    public void queue(SingleChronicleQueue queue) {
        this.queue = queue;
    }

    @Override
    public void onAcquired(int cycle, File file) {
        // no-op
    }

    @Override
    public void onReleased(int cycle, File file) {
        final SingleChronicleQueue q = queue;
        if (q == null)
            return;
        try {
            for (File removable : InternalTailerRetention.analyse(q, keepLastCycles).removable()) {
                if (removable.exists() && removable.delete())
                    Jvm.perf().on(TailerPositionRetentionFileListener.class,
                            "retention: deleted " + removable.getName());
                else
                    break; // stop on first failure so later files stay untouched (ordering matters)
            }
        } catch (Throwable t) {
            // Retention must never destabilise the queue; log and move on.
            Jvm.warn().on(TailerPositionRetentionFileListener.class,
                    "tailer-position retention sweep failed for " + file, t);
        }
    }
}
