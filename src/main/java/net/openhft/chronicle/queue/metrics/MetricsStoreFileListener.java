/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.metrics;

import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.wire.metrics.CounterInstrument;
import org.jetbrains.annotations.NotNull;

import java.io.File;

/**
 * The roll-counting touch point on the existing {@link StoreFileListener} seam: wraps the
 * queue's configured listener and increments the queue's {@link QueueMetrics#ROLLS_TOTAL}
 * counter (labelled {@code queue=<name>}) whenever a store file is acquired (the queue's
 * first open and every cycle roll), then delegates.
 * <p>
 * {@link #wrap(StoreFileListener, String)} is the guard, evaluated once at queue
 * construction (the resolve-once policy, see {@link QueueMetrics}): when no binding is
 * installed - or the {@value QueueMetrics#APPENDER_SOURCE} source is disabled by the
 * binding's source selection - at that moment, the original listener is returned unchanged,
 * nothing is added to the file-acquire path, and a later install is not observed.
 */
public final class MetricsStoreFileListener implements StoreFileListener {

    @NotNull
    private final StoreFileListener delegate;
    private final CounterInstrument rolls;

    private MetricsStoreFileListener(@NotNull StoreFileListener delegate, String queueName) {
        this.delegate = delegate;
        this.rolls = QueueMetrics.rolls(queueName);
    }

    /**
     * Wraps the given listener with roll counting for the given queue if appender metrics
     * are enabled now, otherwise returns it unchanged.
     *
     * @param delegate  the queue's configured listener
     * @param queueName the queue's identity, normally its directory basename
     * @return the listener the queue should use
     */
    public static StoreFileListener wrap(@NotNull StoreFileListener delegate, String queueName) {
        if (delegate instanceof MetricsStoreFileListener || !QueueMetrics.appenderEnabled())
            return delegate;
        return new MetricsStoreFileListener(delegate, queueName);
    }

    @Override
    public boolean isActive() {
        return delegate.isActive();
    }

    @Override
    public void onAcquired(int cycle, File file) {
        rolls.inc();
        delegate.onAcquired(cycle, file);
    }

    @Override
    public void onReleased(int cycle, File file) {
        delegate.onReleased(cycle, file);
    }
}
