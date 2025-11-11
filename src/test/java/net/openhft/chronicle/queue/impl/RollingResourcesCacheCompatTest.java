/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.harness.WeeklyRollCycle;
import org.junit.Test;

import java.io.File;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.queue.rollcycles.LegacyRollCycles.*;
import static org.junit.Assert.assertEquals;

/**
 * Compatibility coverage lifted from adv/code-review branch to boost coverage of
 * RollingResourcesCache date/format arithmetic across epochs, cycles, and formats.
 */
public class RollingResourcesCacheCompatTest extends QueueTestCommon {
    private static final long SEED = 2983472039423847L;

    private static final long AM_EPOCH = 1523498933145L; //2018-04-12 02:08:53.145 UTC
    private static final int AM_DAILY_CYCLE_NUMBER = 1;
    private static final int AM_HOURLY_CYCLE_NUMBER = (AM_DAILY_CYCLE_NUMBER * 24);
    private static final int AM_MINUTELY_CYCLE_NUMBER = (AM_HOURLY_CYCLE_NUMBER * 60);
    private static final String AM_DAILY_FILE_NAME = "20180413";
    private static final String AM_HOURLY_FILE_NAME_0 = "20180413-00";
    private static final String AM_HOURLY_FILE_NAME_15 = "20180413-15";
    private static final String AM_MINUTELY_FILE_NAME_0 = "20180413-0000";
    private static final String AM_MINUTELY_FILE_NAME_10 = "20180413-0010";

    private static final long PM_EPOCH = 1284739200000L; //2010-09-17 16:00:00.000 UTC
    private static final int PM_DAILY_CYCLE_NUMBER = 2484;
    private static final int PM_HOURLY_CYCLE_NUMBER = (PM_DAILY_CYCLE_NUMBER * 24);
    private static final int PM_MINUTELY_CYCLE_NUMBER = (PM_HOURLY_CYCLE_NUMBER * 60);
    private static final String PM_DAILY_FILE_NAME = "20170706";
    private static final String PM_HOURLY_FILE_NAME_0 = "20170706-00";
    private static final String PM_HOURLY_FILE_NAME_15 = "20170706-15";
    private static final String PM_MINUTELY_FILE_NAME_0 = "20170706-0000";
    private static final String PM_MINUTELY_FILE_NAME_10 = "20170706-0010";

    private static final long POSITIVE_RELATIVE_EPOCH = 18000000L; // +5 hours
    private static final int POSITIVE_RELATIVE_DAILY_CYCLE_NUMBER = 2484;
    private static final int POSITIVE_RELATIVE_HOURLY_CYCLE_NUMBER = (POSITIVE_RELATIVE_DAILY_CYCLE_NUMBER * 24);
    private static final int POSITIVE_RELATIVE_MINUTELY_CYCLE_NUMBER = (POSITIVE_RELATIVE_HOURLY_CYCLE_NUMBER * 60);
    private static final String POSITIVE_RELATIVE_DAILY_FILE_NAME = "19761020";
    private static final String POSITIVE_RELATIVE_HOURLY_FILE_NAME_0 = "19761020-00";
    private static final String POSITIVE_RELATIVE_HOURLY_FILE_NAME_15 = "19761020-15";
    private static final String POSITIVE_RELATIVE_MINUTELY_FILE_NAME_0 = "19761020-0000";
    private static final String POSITIVE_RELATIVE_MINUTELY_FILE_NAME_10 = "19761020-0010";

    private static final long BIG_POSITIVE_RELATIVE_EPOCH = 54000000L; // +15 hours
    private static final int BIG_POSITIVE_RELATIVE_DAILY_CYCLE_NUMBER = 2484;
    private static final int BIG_POSITIVE_RELATIVE_HOURLY_CYCLE_NUMBER = (BIG_POSITIVE_RELATIVE_DAILY_CYCLE_NUMBER * 24);
    private static final int BIG_POSITIVE_RELATIVE_MINUTELY_CYCLE_NUMBER = (BIG_POSITIVE_RELATIVE_HOURLY_CYCLE_NUMBER * 60);
    private static final String BIG_POSITIVE_RELATIVE_DAILY_FILE_NAME = "19761020";
    private static final String BIG_POSITIVE_RELATIVE_HOURLY_FILE_NAME_0 = "19761020-00";
    private static final String BIG_POSITIVE_RELATIVE_HOURLY_FILE_NAME_15 = "19761020-15";
    private static final String BIG_POSITIVE_RELATIVE_MINUTELY_FILE_NAME_0 = "19761020-0000";
    private static final String BIG_POSITIVE_RELATIVE_MINUTELY_FILE_NAME_10 = "19761020-0010";

    private static final long NEGATIVE_RELATIVE_EPOCH = -10800000L; // -3 hours
    private static final int NEGATIVE_RELATIVE_DAILY_CYCLE_NUMBER = 2484;
    private static final int NEGATIVE_RELATIVE_HOURLY_CYCLE_NUMBER = (NEGATIVE_RELATIVE_DAILY_CYCLE_NUMBER * 24);
    private static final int NEGATIVE_RELATIVE_MINUTELY_CYCLE_NUMBER = (NEGATIVE_RELATIVE_HOURLY_CYCLE_NUMBER * 60);
    private static final String NEGATIVE_RELATIVE_DAILY_FILE_NAME = "19761019";
    private static final String NEGATIVE_RELATIVE_HOURLY_FILE_NAME_0 = "19761019-00";
    private static final String NEGATIVE_RELATIVE_HOURLY_FILE_NAME_15 = "19761019-15";
    private static final String NEGATIVE_RELATIVE_MINUTELY_FILE_NAME_0 = "19761019-0000";
    private static final String NEGATIVE_RELATIVE_MINUTELY_FILE_NAME_10 = "19761019-0010";

    @Test
    public void testToLong() {
        doTestToLong(DAILY, AM_EPOCH, 0, Long.valueOf("17633"));
        doTestToLong(HOURLY, AM_EPOCH, 0, Long.valueOf("423192"));
        doTestToLong(MINUTELY, AM_EPOCH, 0, Long.valueOf("25391520"));
        doTestToLong(DAILY, AM_EPOCH, 100, Long.valueOf("17733"));
        doTestToLong(HOURLY, AM_EPOCH, 100, Long.valueOf("423292"));
        doTestToLong(MINUTELY, AM_EPOCH, 100, Long.valueOf("25391620"));
        doTestToLong(WeeklyRollCycle.INSTANCE, AM_EPOCH, 0, Long.valueOf("2519"));

        doTestToLong(DAILY, PM_EPOCH, 0, Long.valueOf("14869"));
        doTestToLong(HOURLY, PM_EPOCH, 0, Long.valueOf("356856"));
        doTestToLong(MINUTELY, PM_EPOCH, 0, Long.valueOf("21411360"));
        doTestToLong(DAILY, PM_EPOCH, 100, Long.valueOf("14969"));
        doTestToLong(HOURLY, PM_EPOCH, 100, Long.valueOf("356956"));
        doTestToLong(MINUTELY, PM_EPOCH, 100, Long.valueOf("21411460"));
        doTestToLong(WeeklyRollCycle.INSTANCE, PM_EPOCH, 0, Long.valueOf("2124"));

        doTestToLong(DAILY, POSITIVE_RELATIVE_EPOCH, 0, Long.valueOf("0"));
        doTestToLong(HOURLY, POSITIVE_RELATIVE_EPOCH, 0, Long.valueOf("0"));
        doTestToLong(MINUTELY, POSITIVE_RELATIVE_EPOCH, 0, Long.valueOf("0"));
        doTestToLong(DAILY, POSITIVE_RELATIVE_EPOCH, 100, Long.valueOf("100"));
        doTestToLong(HOURLY, POSITIVE_RELATIVE_EPOCH, 100, Long.valueOf("100"));
        doTestToLong(MINUTELY, POSITIVE_RELATIVE_EPOCH, 100, Long.valueOf("100"));
        doTestToLong(WeeklyRollCycle.INSTANCE, POSITIVE_RELATIVE_EPOCH, 7, Long.valueOf("7"));

        doTestToLong(DAILY, NEGATIVE_RELATIVE_EPOCH, 0, Long.valueOf("-1"));
        doTestToLong(HOURLY, NEGATIVE_RELATIVE_EPOCH, 0, Long.valueOf("-24"));
        doTestToLong(MINUTELY, NEGATIVE_RELATIVE_EPOCH, 0, Long.valueOf("-1440"));
        doTestToLong(DAILY, NEGATIVE_RELATIVE_EPOCH, 100, Long.valueOf("99"));
        doTestToLong(HOURLY, NEGATIVE_RELATIVE_EPOCH, 100, Long.valueOf("76"));
        doTestToLong(MINUTELY, NEGATIVE_RELATIVE_EPOCH, 100, Long.valueOf("-1340"));
        doTestToLong(WeeklyRollCycle.INSTANCE, NEGATIVE_RELATIVE_EPOCH, 0, Long.valueOf("-1"));

    }

    public void doTestToLong(RollCycle rollCycle, long epoch, long cycle, Long expectedLong) {
        RollingResourcesCache cache =
                new RollingResourcesCache(rollCycle, epoch, File::new, File::getName);

        RollingResourcesCache.Resource resource = cache.resourceFor(cycle);
        assertEquals(expectedLong, cache.toLong(resource.path));
    }
}
