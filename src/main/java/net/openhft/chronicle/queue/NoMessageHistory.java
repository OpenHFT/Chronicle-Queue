/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.wire.*;

// Is this being used?
public enum NoMessageHistory implements MessageHistory {
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
