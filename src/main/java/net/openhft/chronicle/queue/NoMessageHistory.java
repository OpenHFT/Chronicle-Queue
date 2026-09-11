/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.wire.*;

/**
 * {@link MessageHistory} implementation that records no timing or source information.
 * <p>
 * This singleton can be used in code paths where message history is either disabled for
 * performance reasons or not relevant to the use case. All accessors return sentinel values
 * and {@link #reset(int, long)} is a no-op.
 */
@Deprecated(/* to be removed in 2027, only used in tests */)
public enum NoMessageHistory implements MessageHistory {
    /**
     * Singleton instance that records no message history.
     */
    INSTANCE;

    @Override
    public int timings() {
        return 0;
    }

    @Override
    public long timing(int n) {
        return -1;
    }

    @Override
    public int sources() {
        return 0;
    }

    @Override
    public int sourceId(int n) {
        return -1;
    }

    @Override
    public boolean sourceIdsEndsWith(int[] sourceIds) {
        return false;
    }

    @Override
    public long sourceIndex(int n) {
        return -1;
    }

    @Override
    public void reset(int sourceId, long sourceIndex) {
        // ignored
    }

    @Override
    public void reset() {
        // no-op
    }

    @Override
    public int lastSourceId() {
        return -1;
    }

    @Override
    public long lastSourceIndex() {
        return -1;
    }

    @Override
    public boolean isDirty() {
        return false;
    }
}
