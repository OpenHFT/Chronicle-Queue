/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.metrics;

import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.wire.metrics.CounterInstrument;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The roll-counting touch point on the existing {@link StoreFileListener} seam: wraps the
 * queue's configured listener and increments the queue's {@link QueueMetrics#ROLLS_TOTAL}
 * counter (labelled {@code queue=<name>}) when a store file is acquired for a cycle
 * <em>newer than any seen before</em> (the queue's first open and every forward cycle
 * roll), then delegates.
 * <p>
 * The forward-only guard matters because {@code onAcquired} fires for <em>every</em> store
 * acquisition by the queue's pool - tailers replaying old cycles, metadata queries such as
 * first/last-cycle scans, and re-acquisitions of the current cycle - none of which are
 * rolls. A queue idle across several cycles counts one roll on the next append (the counter
 * counts transitions, not elapsed cycles).
 * <p>
 * {@link #wrap(StoreFileListener, String)} is the guard, evaluated once at queue
 * construction (the resolve-once policy, see {@link QueueMetrics}): when no binding is
 * installed - or the {@value QueueMetrics#APPENDER_SOURCE} source is disabled by the
 * binding's source selection - at that moment, the original listener is returned unchanged,
 * nothing is added to the file-acquire path, and a later install is not observed. Once
 * wrapped, this listener is active even when the delegate is a no-op listener, because the
 * wrapper itself needs roll callbacks to emit {@value QueueMetrics#ROLLS_TOTAL}.
 */
public final class MetricsStoreFileListener implements StoreFileListener {

    @NotNull
    private final StoreFileListener delegate;
    private final CounterInstrument rolls;
    // highest cycle seen; onAcquired may be called from appender and tailer threads
    private final AtomicInteger maxCycleSeen = new AtomicInteger(Integer.MIN_VALUE);

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
        return true;
    }

    @Override
    public void onAcquired(int cycle, File file) {
        // count only forward rolls: at most one thread observes prev < cycle per transition
        final int prev = maxCycleSeen.getAndAccumulate(cycle, Math::max);
        if (cycle > prev)
            rolls.inc();
        delegate.onAcquired(cycle, file);
    }

    @Override
    public void onReleased(int cycle, File file) {
        delegate.onReleased(cycle, file);
    }
}
