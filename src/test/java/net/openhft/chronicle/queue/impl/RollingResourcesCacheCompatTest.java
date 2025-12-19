/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl;

import org.junit.jupiter.api.Test;

import java.io.File;

import static net.openhft.chronicle.queue.rollcycles.LegacyRollCycles.*;
import static net.openhft.chronicle.queue.harness.WeeklyRollCycle.INSTANCE;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Compatibility coverage lifted from adv/code-review branch to boost coverage of
 * RollingResourcesCache date/format arithmetic across epochs, cycles, and formats.
 */
public class RollingResourcesCacheCompatTest extends RollingResourcesCacheTestBase {

    @Override
    protected long getAmEpoch() {
        return AM_EPOCH;
    }

    @Override
    protected long getPmEpoch() {
        return PM_EPOCH;
    }

    @Override
    protected long getPositiveRelativeEpoch() {
        return POSITIVE_RELATIVE_EPOCH;
    }

    @Override
    protected long getNegativeRelativeEpoch() {
        return NEGATIVE_RELATIVE_EPOCH;
    }

    @Test
    public void testToLong() {
        RollingResourcesCache cache = new RollingResourcesCache(DAILY, getAmEpoch(), File::new, File::getName);
        RollingResourcesCache.Resource resource = cache.resourceFor(0);
        assertEquals(Long.valueOf("17633"), cache.toLong(resource.path), "toLong: daily am cycle 0");
        runStandardToLongTests();
    }
}
