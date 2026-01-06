/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.harness.WeeklyRollCycle;

import java.io.File;

import static net.openhft.chronicle.queue.rollcycles.LegacyRollCycles.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Base class for RollingResourcesCache tests, containing shared test helper methods and constants.
 */
public abstract class RollingResourcesCacheTestBase extends QueueTestCommon {

    protected static final long SEED = 2983472039423847L;

    protected static final long AM_EPOCH = 1523498933145L; //2018-04-12 02:08:53.145 UTC
    protected static final int AM_DAILY_CYCLE_NUMBER = 1;
    protected static final int AM_HOURLY_CYCLE_NUMBER = (AM_DAILY_CYCLE_NUMBER * 24);
    protected static final int AM_MINUTELY_CYCLE_NUMBER = (AM_HOURLY_CYCLE_NUMBER * 60);
    protected static final String AM_DAILY_FILE_NAME = "20180413";
    protected static final String AM_HOURLY_FILE_NAME_0 = "20180413-00";
    protected static final String AM_HOURLY_FILE_NAME_15 = "20180413-15";
    protected static final String AM_MINUTELY_FILE_NAME_0 = "20180413-0000";
    protected static final String AM_MINUTELY_FILE_NAME_10 = "20180413-0010";

    protected static final long PM_EPOCH = 1284739200000L; //2010-09-17 16:00:00.000 UTC
    protected static final int PM_DAILY_CYCLE_NUMBER = 2484;
    protected static final int PM_HOURLY_CYCLE_NUMBER = (PM_DAILY_CYCLE_NUMBER * 24);
    protected static final int PM_MINUTELY_CYCLE_NUMBER = (PM_HOURLY_CYCLE_NUMBER * 60);
    protected static final String PM_DAILY_FILE_NAME = "20170706";
    protected static final String PM_HOURLY_FILE_NAME_0 = "20170706-00";
    protected static final String PM_HOURLY_FILE_NAME_15 = "20170706-15";
    protected static final String PM_MINUTELY_FILE_NAME_0 = "20170706-0000";
    protected static final String PM_MINUTELY_FILE_NAME_10 = "20170706-0010";

    protected static final long POSITIVE_RELATIVE_EPOCH = 18000000L; // +5 hours
    protected static final int POSITIVE_RELATIVE_DAILY_CYCLE_NUMBER = 2484;
    protected static final int POSITIVE_RELATIVE_HOURLY_CYCLE_NUMBER = (POSITIVE_RELATIVE_DAILY_CYCLE_NUMBER * 24);
    protected static final int POSITIVE_RELATIVE_MINUTELY_CYCLE_NUMBER = (POSITIVE_RELATIVE_HOURLY_CYCLE_NUMBER * 60);
    protected static final String POSITIVE_RELATIVE_DAILY_FILE_NAME = "19761020";
    protected static final String POSITIVE_RELATIVE_HOURLY_FILE_NAME_0 = "19761020-00";
    protected static final String POSITIVE_RELATIVE_HOURLY_FILE_NAME_15 = "19761020-15";
    protected static final String POSITIVE_RELATIVE_MINUTELY_FILE_NAME_0 = "19761020-0000";
    protected static final String POSITIVE_RELATIVE_MINUTELY_FILE_NAME_10 = "19761020-0010";

    protected static final long BIG_POSITIVE_RELATIVE_EPOCH = 54000000L; // +15 hours
    protected static final int BIG_POSITIVE_RELATIVE_DAILY_CYCLE_NUMBER = 2484;
    protected static final int BIG_POSITIVE_RELATIVE_HOURLY_CYCLE_NUMBER = (BIG_POSITIVE_RELATIVE_DAILY_CYCLE_NUMBER * 24);
    protected static final int BIG_POSITIVE_RELATIVE_MINUTELY_CYCLE_NUMBER = (BIG_POSITIVE_RELATIVE_HOURLY_CYCLE_NUMBER * 60);
    protected static final String BIG_POSITIVE_RELATIVE_DAILY_FILE_NAME = "19761020";
    protected static final String BIG_POSITIVE_RELATIVE_HOURLY_FILE_NAME_0 = "19761020-00";
    protected static final String BIG_POSITIVE_RELATIVE_HOURLY_FILE_NAME_15 = "19761020-15";
    protected static final String BIG_POSITIVE_RELATIVE_MINUTELY_FILE_NAME_0 = "19761020-0000";
    protected static final String BIG_POSITIVE_RELATIVE_MINUTELY_FILE_NAME_10 = "19761020-0010";

    protected static final long NEGATIVE_RELATIVE_EPOCH = -10800000L; // -3 hours
    protected static final int NEGATIVE_RELATIVE_DAILY_CYCLE_NUMBER = 2484;
    protected static final int NEGATIVE_RELATIVE_HOURLY_CYCLE_NUMBER = (NEGATIVE_RELATIVE_DAILY_CYCLE_NUMBER * 24);
    protected static final int NEGATIVE_RELATIVE_MINUTELY_CYCLE_NUMBER = (NEGATIVE_RELATIVE_HOURLY_CYCLE_NUMBER * 60);
    protected static final String NEGATIVE_RELATIVE_DAILY_FILE_NAME = "19761019";
    protected static final String NEGATIVE_RELATIVE_HOURLY_FILE_NAME_0 = "19761019-00";
    protected static final String NEGATIVE_RELATIVE_HOURLY_FILE_NAME_15 = "19761019-15";
    protected static final String NEGATIVE_RELATIVE_MINUTELY_FILE_NAME_0 = "19761019-0000";
    protected static final String NEGATIVE_RELATIVE_MINUTELY_FILE_NAME_10 = "19761019-0010";

    /**
     * Subclasses must provide their epoch constants for the standard test suite.
     */
    protected abstract long getAmEpoch();

    protected abstract long getPmEpoch();

    protected abstract long getPositiveRelativeEpoch();

    protected abstract long getNegativeRelativeEpoch();

    /**
     * Shared test method that verifies RollingResourcesCache.toLong() across
     * different roll cycles, epochs, and cycle numbers.
     */
    protected void runStandardToLongTests() {
        doTestToLong(DAILY, getAmEpoch(), 0, Long.valueOf("17633"));
        doTestToLong(HOURLY, getAmEpoch(), 0, Long.valueOf("423192"));
        doTestToLong(MINUTELY, getAmEpoch(), 0, Long.valueOf("25391520"));
        doTestToLong(DAILY, getAmEpoch(), 100, Long.valueOf("17733"));
        doTestToLong(HOURLY, getAmEpoch(), 100, Long.valueOf("423292"));
        doTestToLong(MINUTELY, getAmEpoch(), 100, Long.valueOf("25391620"));
        doTestToLong(WeeklyRollCycle.INSTANCE, getAmEpoch(), 0, Long.valueOf("2519"));

        doTestToLong(DAILY, getPmEpoch(), 0, Long.valueOf("14869"));
        doTestToLong(HOURLY, getPmEpoch(), 0, Long.valueOf("356856"));
        doTestToLong(MINUTELY, getPmEpoch(), 0, Long.valueOf("21411360"));
        doTestToLong(DAILY, getPmEpoch(), 100, Long.valueOf("14969"));
        doTestToLong(HOURLY, getPmEpoch(), 100, Long.valueOf("356956"));
        doTestToLong(MINUTELY, getPmEpoch(), 100, Long.valueOf("21411460"));
        doTestToLong(WeeklyRollCycle.INSTANCE, getPmEpoch(), 0, Long.valueOf("2124"));

        doTestToLong(DAILY, getPositiveRelativeEpoch(), 0, Long.valueOf("0"));
        doTestToLong(HOURLY, getPositiveRelativeEpoch(), 0, Long.valueOf("0"));
        doTestToLong(MINUTELY, getPositiveRelativeEpoch(), 0, Long.valueOf("0"));
        doTestToLong(DAILY, getPositiveRelativeEpoch(), 100, Long.valueOf("100"));
        doTestToLong(HOURLY, getPositiveRelativeEpoch(), 100, Long.valueOf("100"));
        doTestToLong(MINUTELY, getPositiveRelativeEpoch(), 100, Long.valueOf("100"));
        doTestToLong(WeeklyRollCycle.INSTANCE, getPositiveRelativeEpoch(), 7, Long.valueOf("7"));

        doTestToLong(DAILY, getNegativeRelativeEpoch(), 0, Long.valueOf("-1"));
        doTestToLong(HOURLY, getNegativeRelativeEpoch(), 0, Long.valueOf("-24"));
        doTestToLong(MINUTELY, getNegativeRelativeEpoch(), 0, Long.valueOf("-1440"));
        doTestToLong(DAILY, getNegativeRelativeEpoch(), 100, Long.valueOf("99"));
        doTestToLong(HOURLY, getNegativeRelativeEpoch(), 100, Long.valueOf("76"));
        doTestToLong(MINUTELY, getNegativeRelativeEpoch(), 100, Long.valueOf("-1340"));
        doTestToLong(WeeklyRollCycle.INSTANCE, getNegativeRelativeEpoch(), 0, Long.valueOf("-1"));
    }

    /**
     * Shared helper method to test RollingResourcesCache.toLong() for a given
     * roll cycle, epoch, cycle number, and expected result.
     */
    protected void doTestToLong(RollCycle rollCycle, long epoch, long cycle, Long expectedLong) {
        RollingResourcesCache cache =
                new RollingResourcesCache(rollCycle, epoch, File::new, File::getName);

        RollingResourcesCache.Resource resource = cache.resourceFor(cycle);
        assertEquals(expectedLong, cache.toLong(resource.path), "toLong should parse resource path");
    }
}
