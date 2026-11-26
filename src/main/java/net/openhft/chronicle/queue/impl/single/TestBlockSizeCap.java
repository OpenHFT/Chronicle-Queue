/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

/** Explicit test-JVM policy; captured once for each new builder. */
final class TestBlockSizeCap {
    private static final String PROPERTY = "chronicle.queue.maxBlockSize";

    private TestBlockSizeCap() {
    }

    static long configured() {
        if (!Boolean.getBoolean("chronicle.queue.testMode"))
            return 0;
        String value = System.getProperty(PROPERTY);
        if (value == null)
            return 0;
        final long cap;
        try {
            cap = Long.parseLong(value);
        } catch (NumberFormatException invalid) {
            throw new IllegalArgumentException(PROPERTY + " must be a positive decimal byte count: " + value, invalid);
        }
        if (cap <= 0)
            throw new IllegalArgumentException(PROPERTY + " must be positive: " + value);
        return cap;
    }

    static long apply(long requested, long minimum, long cap) {
        long effective = Math.max(minimum, requested);
        if (cap == 0)
            return effective;
        if (cap < minimum)
            throw new IllegalArgumentException(PROPERTY + "=" + cap
                    + " is below the required minimum blockSize=" + minimum
                    + "; increase the test cap or reduce the index geometry");
        return Math.min(effective, cap);
    }
}
