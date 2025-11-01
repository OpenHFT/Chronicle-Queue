/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.harness.WeeklyRollCycle;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import org.junit.Test;

import java.io.File;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.WeekFields;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.queue.rollcycles.LegacyRollCycles.*;
import static org.junit.Assert.*;

public class RollingResourcesCacheTest extends QueueTestCommon {

    // Additional coverage constants mirrored from adv/code-review
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
    public void resourceLookupIsCached() {
        final File dir = getTmpDir();
        final RollingResourcesCache cache = newCache(dir);

        final RollingResourcesCache.Resource first = cache.resourceFor(0);
        final RollingResourcesCache.Resource repeat = cache.resourceFor(0);
        final RollingResourcesCache.Resource next = cache.resourceFor(1);

        assertSame("Expected identical instance for cached cycle", first, repeat);
        assertNotSame("Different cycle should produce a new resource", first, next);

        final int firstCount = cache.parseCount(first.text);
        final int cachedCount = cache.parseCount(first.text);
        assertEquals(firstCount, cachedCount);
    }

    @Test
    public void toLongCachesAndClearsWhenFull() {
        final File dir = getTmpDir();
        final RollingResourcesCache cache = newCache(dir);

        final RollingResourcesCache.Resource base = cache.resourceFor(0);
        final File baseFile = new File(dir, base.text + ".cq4");
        final Long initial = cache.toLong(baseFile);
        assertNotNull(initial);

        // Populate with enough unique entries to trigger cache eviction logic
        for (int i = 1; i < 40; i++) {
            final RollingResourcesCache.Resource resource = cache.resourceFor(i);
            final File file = new File(dir, resource.text + ".cq4");
            assertNotNull(cache.toLong(file));
        }

        final Long afterEviction = cache.toLong(baseFile);
        assertEquals(initial, afterEviction);
    }

    @Test
    public void parseWeeklyFormatValid() {
        // round-trip property: resourceFor(cycle).text parses back to the same cycle
        final RollingResourcesCache weeklyCache = new RollingResourcesCache(
                WeeklyRollCycle.INSTANCE, 0, File::new, File::getName);
        int base = (int) (System.currentTimeMillis() / TimeUnit.DAYS.toMillis(1));
        for (int i = 0; i < 50; i++) {
            int cycle = base + i;
            String name = weeklyCache.resourceFor(cycle).text;
            int parsed = weeklyCache.parseCount(name);
            assertEquals(cycle, parsed);
        }
    }

    @Test
    public void shouldConvertCyclesToResourceNamesWithNoEpoch() {
        final int epoch = 0;
        final RollingResourcesCache cache =
                new RollingResourcesCache(DAILY, epoch, File::new, File::getName);

        final int cycle = DAILY.current(System::currentTimeMillis, 0);
        final String name = cache.resourceFor(cycle).text;
        final int parsed = cache.parseCount(name);
        assertEquals(cycle, parsed);
    }

    @Test
    public void shouldCorrectlyConvertCyclesToResourceNamesWithEpoch() {
        doTestCycleAndResourceNames(AM_EPOCH, DAILY, AM_DAILY_CYCLE_NUMBER, AM_DAILY_FILE_NAME);
        doTestCycleAndResourceNames(AM_EPOCH, HOURLY, AM_HOURLY_CYCLE_NUMBER, AM_HOURLY_FILE_NAME_0);
        doTestCycleAndResourceNames(AM_EPOCH, HOURLY, AM_HOURLY_CYCLE_NUMBER + 15, AM_HOURLY_FILE_NAME_15);
        doTestCycleAndResourceNames(AM_EPOCH, MINUTELY, AM_MINUTELY_CYCLE_NUMBER, AM_MINUTELY_FILE_NAME_0);
        doTestCycleAndResourceNames(AM_EPOCH, MINUTELY, AM_MINUTELY_CYCLE_NUMBER + 10, AM_MINUTELY_FILE_NAME_10);
        // Weekly roll cycle: verify round-trip
        doRoundTripForWeekly(AM_EPOCH, AM_DAILY_CYCLE_NUMBER);

        doTestCycleAndResourceNames(PM_EPOCH, DAILY, PM_DAILY_CYCLE_NUMBER, PM_DAILY_FILE_NAME);
        doTestCycleAndResourceNames(PM_EPOCH, HOURLY, PM_HOURLY_CYCLE_NUMBER, PM_HOURLY_FILE_NAME_0);
        doTestCycleAndResourceNames(PM_EPOCH, HOURLY, PM_HOURLY_CYCLE_NUMBER + 15, PM_HOURLY_FILE_NAME_15);
        doTestCycleAndResourceNames(PM_EPOCH, MINUTELY, PM_MINUTELY_CYCLE_NUMBER, PM_MINUTELY_FILE_NAME_0);
        doTestCycleAndResourceNames(PM_EPOCH, MINUTELY, PM_MINUTELY_CYCLE_NUMBER + 10, PM_MINUTELY_FILE_NAME_10);
        doRoundTripForWeekly(PM_EPOCH, 42);
    }

    @Test
    public void positiveRelativeEpochShouldWork() {
        doTestCycleAndResourceNames(POSITIVE_RELATIVE_EPOCH, DAILY, POSITIVE_RELATIVE_DAILY_CYCLE_NUMBER, POSITIVE_RELATIVE_DAILY_FILE_NAME);
        doTestCycleAndResourceNames(POSITIVE_RELATIVE_EPOCH, HOURLY, POSITIVE_RELATIVE_HOURLY_CYCLE_NUMBER, POSITIVE_RELATIVE_HOURLY_FILE_NAME_0);
        doTestCycleAndResourceNames(POSITIVE_RELATIVE_EPOCH, HOURLY, POSITIVE_RELATIVE_HOURLY_CYCLE_NUMBER + 15, POSITIVE_RELATIVE_HOURLY_FILE_NAME_15);
        doTestCycleAndResourceNames(POSITIVE_RELATIVE_EPOCH, MINUTELY, POSITIVE_RELATIVE_MINUTELY_CYCLE_NUMBER, POSITIVE_RELATIVE_MINUTELY_FILE_NAME_0);
        doTestCycleAndResourceNames(POSITIVE_RELATIVE_EPOCH, MINUTELY, POSITIVE_RELATIVE_MINUTELY_CYCLE_NUMBER + 10, POSITIVE_RELATIVE_MINUTELY_FILE_NAME_10);
        doRoundTripForWeekly(POSITIVE_RELATIVE_EPOCH, 2483);
    }

    @Test
    public void bigPositiveRelativeEpochShouldWork() {
        doTestCycleAndResourceNames(BIG_POSITIVE_RELATIVE_EPOCH, DAILY, BIG_POSITIVE_RELATIVE_DAILY_CYCLE_NUMBER, BIG_POSITIVE_RELATIVE_DAILY_FILE_NAME);
        doTestCycleAndResourceNames(BIG_POSITIVE_RELATIVE_EPOCH, HOURLY, BIG_POSITIVE_RELATIVE_HOURLY_CYCLE_NUMBER, BIG_POSITIVE_RELATIVE_HOURLY_FILE_NAME_0);
        doTestCycleAndResourceNames(BIG_POSITIVE_RELATIVE_EPOCH, HOURLY, BIG_POSITIVE_RELATIVE_HOURLY_CYCLE_NUMBER + 15, BIG_POSITIVE_RELATIVE_HOURLY_FILE_NAME_15);
        doTestCycleAndResourceNames(BIG_POSITIVE_RELATIVE_EPOCH, MINUTELY, BIG_POSITIVE_RELATIVE_MINUTELY_CYCLE_NUMBER, BIG_POSITIVE_RELATIVE_MINUTELY_FILE_NAME_0);
        doTestCycleAndResourceNames(BIG_POSITIVE_RELATIVE_EPOCH, MINUTELY, BIG_POSITIVE_RELATIVE_MINUTELY_CYCLE_NUMBER + 10, BIG_POSITIVE_RELATIVE_MINUTELY_FILE_NAME_10);
        doRoundTripForWeekly(BIG_POSITIVE_RELATIVE_EPOCH, 2484);
    }

    @Test
    public void negativeRelativeEpochShouldWork() {
        doTestCycleAndResourceNames(NEGATIVE_RELATIVE_EPOCH, DAILY, NEGATIVE_RELATIVE_DAILY_CYCLE_NUMBER, NEGATIVE_RELATIVE_DAILY_FILE_NAME);
        doTestCycleAndResourceNames(NEGATIVE_RELATIVE_EPOCH, HOURLY, NEGATIVE_RELATIVE_HOURLY_CYCLE_NUMBER, NEGATIVE_RELATIVE_HOURLY_FILE_NAME_0);
        doTestCycleAndResourceNames(NEGATIVE_RELATIVE_EPOCH, HOURLY, NEGATIVE_RELATIVE_HOURLY_CYCLE_NUMBER + 15, NEGATIVE_RELATIVE_HOURLY_FILE_NAME_15);
        doTestCycleAndResourceNames(NEGATIVE_RELATIVE_EPOCH, MINUTELY, NEGATIVE_RELATIVE_MINUTELY_CYCLE_NUMBER, NEGATIVE_RELATIVE_MINUTELY_FILE_NAME_0);
        doTestCycleAndResourceNames(NEGATIVE_RELATIVE_EPOCH, MINUTELY, NEGATIVE_RELATIVE_MINUTELY_CYCLE_NUMBER + 10, NEGATIVE_RELATIVE_MINUTELY_FILE_NAME_10);
        doRoundTripForWeekly(NEGATIVE_RELATIVE_EPOCH, 2484);
    }

    private void doTestCycleAndResourceNames(long epoch, RollCycle rollCycle, int cycleNumber, String filename) {
        RollingResourcesCache cache =
                new RollingResourcesCache(rollCycle, epoch, File::new, File::getName);
        assertEquals(filename, cache.resourceFor(cycleNumber).text);
        assertEquals(cycleNumber, cache.parseCount(filename));
    }

    private void assertCorrectConversion(RollingResourcesCache cache, int cycle) {
        String name = cache.resourceFor(cycle).text;
        int parsed = cache.parseCount(name);
        // round-trip checks
        assertEquals(cycle, parsed);
    }

    private void doRoundTripForWeekly(long epoch, int cycleNumber) {
        RollingResourcesCache cache =
                new RollingResourcesCache(WeeklyRollCycle.INSTANCE, epoch, File::new, File::getName);
        assertCorrectConversion(cache, cycleNumber);
    }

    @Test(expected = RuntimeException.class)
    public void parseInvalidFormatThrows() {
        final File dir = getTmpDir();
        final RollingResourcesCache cache = new RollingResourcesCache(
                TestRollCycles.TEST_SECONDLY,
                0,
                name -> new File(dir, name + ".cq4"),
                file -> file.getName().replaceFirst("\\.cq4$", "")
        );

        cache.parseCount("not-a-valid-name");
    }

    private RollingResourcesCache newCache(File dir) {
        return new RollingResourcesCache(
                TestRollCycles.TEST_SECONDLY,
                0,
                name -> new File(dir, name + ".cq4"),
                file -> file.getName().replaceFirst("\\.cq4$", "")
        );
    }
}
