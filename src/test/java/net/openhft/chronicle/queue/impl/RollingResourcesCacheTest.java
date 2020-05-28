package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.harness.WeeklyRollCycle;
import org.junit.Test;

import java.io.File;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class RollingResourcesCacheTest {
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

    private static final long BIG_NEGATIVE_RELATIVE_EPOCH = -10800000L; // -13 hours
    private static final int BIG_NEGATIVE_RELATIVE_DAILY_CYCLE_NUMBER = 2484;
    private static final int BIG_NEGATIVE_RELATIVE_HOURLY_CYCLE_NUMBER = (BIG_NEGATIVE_RELATIVE_DAILY_CYCLE_NUMBER * 24);
    private static final int BIG_NEGATIVE_RELATIVE_MINUTELY_CYCLE_NUMBER = (BIG_NEGATIVE_RELATIVE_HOURLY_CYCLE_NUMBER * 60);
    private static final String BIG_NEGATIVE_RELATIVE_DAILY_FILE_NAME = "19761019";
    private static final String BIG_NEGATIVE_RELATIVE_HOURLY_FILE_NAME_0 = "19761019-00";
    private static final String BIG_NEGATIVE_RELATIVE_HOURLY_FILE_NAME_15 = "19761019-15";
    private static final String BIG_NEGATIVE_RELATIVE_MINUTELY_FILE_NAME_0 = "19761019-0000";
    private static final String BIG_NEGATIVE_RELATIVE_MINUTELY_FILE_NAME_10 = "19761019-0010";

    private static final long ONE_DAY_IN_MILLIS = TimeUnit.DAYS.toMillis(1L);
    private static final boolean LOG_TEST_DEBUG =
            Boolean.valueOf(RollingResourcesCacheTest.class.getSimpleName() + ".debug");

    private static void assertCorrectConversion(final RollingResourcesCache cache, final int cycle,
                                                final Instant instant, final DateTimeFormatter formatter) {
        final String expectedFileName = formatter.format(instant);
        assertThat(cache.resourceFor(cycle).text, is(expectedFileName));
        assertThat(cache.parseCount(expectedFileName), is(cycle));
    }

    @Test
    public void shouldConvertCyclesToResourceNamesWithNoEpoch() {
        final int epoch = 0;
        final RollingResourcesCache cache =
                new RollingResourcesCache(RollCycles.DAILY, epoch, File::new, File::getName);

        final int cycle = RollCycles.DAILY.current(System::currentTimeMillis, 0);
        assertCorrectConversion(cache, cycle, Instant.now(),
                DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneId.of("GMT")));
    }

    private void doTestCycleAndResourceNames(long epoch, RollCycle rollCycle, int cycleNumber, String filename) {
        RollingResourcesCache cache =
                new RollingResourcesCache(rollCycle, epoch, File::new, File::getName);
        assertThat(cache.resourceFor(cycleNumber).text, is(filename));
        assertThat(cache.parseCount(filename), is(cycleNumber));
    }

    @Test
    public void shouldCorrectlyConvertCyclesToResourceNamesWithEpoch() {
        // AM_EPOCH is 2018-04-12 02:08:53.145 UTC
        // cycle 24 should be formatted as:
        // 2018-04-12 00:00:00 UTC (1523491200000) +
        // Timezone offset 02:08:53.145 (7733145) +
        // 24 hourly cycles (24 * 3_600_000) =
        // 1523585333145 = Friday, 13 April 2018 02:08:53.145 UTC ie. filename is 20180413 local
        doTestCycleAndResourceNames(AM_EPOCH, RollCycles.DAILY, AM_DAILY_CYCLE_NUMBER, AM_DAILY_FILE_NAME);
        doTestCycleAndResourceNames(AM_EPOCH, RollCycles.HOURLY, AM_HOURLY_CYCLE_NUMBER, AM_HOURLY_FILE_NAME_0);
        doTestCycleAndResourceNames(AM_EPOCH, RollCycles.HOURLY, AM_HOURLY_CYCLE_NUMBER + 15, AM_HOURLY_FILE_NAME_15);
        doTestCycleAndResourceNames(AM_EPOCH, RollCycles.MINUTELY, AM_MINUTELY_CYCLE_NUMBER, AM_MINUTELY_FILE_NAME_0);
        doTestCycleAndResourceNames(AM_EPOCH, RollCycles.MINUTELY, AM_MINUTELY_CYCLE_NUMBER + 10, AM_MINUTELY_FILE_NAME_10);
        doTestCycleAndResourceNames(AM_EPOCH, WeeklyRollCycle.INSTANCE, AM_DAILY_CYCLE_NUMBER, "2018109");

        // PM_EPOCH is 2010-09-17 16:00:00.000 UTC
        // cycle 2484 should be formatted as:
        // 2010-09-17 00:00:00 UTC (1284681600000) +
        // Timezone offset 16:00:00.000 (57600000) +
        // 2484 daily cycles (2484 * 86_400_000 = 214617600000)
        // 1499356800000 = Thursday, 6 July 2017 16:00:00 UTC ie. filename is 20170706 local
        doTestCycleAndResourceNames(PM_EPOCH, RollCycles.DAILY, PM_DAILY_CYCLE_NUMBER, PM_DAILY_FILE_NAME);
        doTestCycleAndResourceNames(PM_EPOCH, RollCycles.HOURLY, PM_HOURLY_CYCLE_NUMBER, PM_HOURLY_FILE_NAME_0);
        doTestCycleAndResourceNames(PM_EPOCH, RollCycles.HOURLY, PM_HOURLY_CYCLE_NUMBER + 15, PM_HOURLY_FILE_NAME_15);
        doTestCycleAndResourceNames(PM_EPOCH, RollCycles.MINUTELY, PM_MINUTELY_CYCLE_NUMBER, PM_MINUTELY_FILE_NAME_0);
        doTestCycleAndResourceNames(PM_EPOCH, RollCycles.MINUTELY, PM_MINUTELY_CYCLE_NUMBER + 10, PM_MINUTELY_FILE_NAME_10);
        doTestCycleAndResourceNames(PM_EPOCH, WeeklyRollCycle.INSTANCE, 42, "2011189");

        // POSITIVE_RELATIVE_EPOCH is 5 hours (18000000 millis)
        // cycle 2484 should be formatted as:
        // epoch 1970-01-01 00:00:00 (0) +
        // Timezone offset 05:00:00 (18000000) +
        // 2484 daily cycles (2484 * 86_400_000 = 214617600000) =
        // 214635600000 - Wednesday, 20 October 1976 05:00:00 UTC ie. filename is 19761020 local
        doTestCycleAndResourceNames(POSITIVE_RELATIVE_EPOCH, RollCycles.DAILY, POSITIVE_RELATIVE_DAILY_CYCLE_NUMBER, POSITIVE_RELATIVE_DAILY_FILE_NAME);
        doTestCycleAndResourceNames(POSITIVE_RELATIVE_EPOCH, RollCycles.HOURLY, POSITIVE_RELATIVE_HOURLY_CYCLE_NUMBER, POSITIVE_RELATIVE_HOURLY_FILE_NAME_0);
        doTestCycleAndResourceNames(POSITIVE_RELATIVE_EPOCH, RollCycles.HOURLY, POSITIVE_RELATIVE_HOURLY_CYCLE_NUMBER + 15, POSITIVE_RELATIVE_HOURLY_FILE_NAME_15);
        doTestCycleAndResourceNames(POSITIVE_RELATIVE_EPOCH, RollCycles.MINUTELY, POSITIVE_RELATIVE_MINUTELY_CYCLE_NUMBER, POSITIVE_RELATIVE_MINUTELY_FILE_NAME_0);
        doTestCycleAndResourceNames(POSITIVE_RELATIVE_EPOCH, RollCycles.MINUTELY, POSITIVE_RELATIVE_MINUTELY_CYCLE_NUMBER + 10, POSITIVE_RELATIVE_MINUTELY_FILE_NAME_10);
        doTestCycleAndResourceNames(POSITIVE_RELATIVE_EPOCH, WeeklyRollCycle.INSTANCE, 354, "1976288");

        // BIG_POSITIVE_RELATIVE_EPOCH is 15 hours (54000000 millis)
        // cycle 2484 should be formatted as:
        // epoch 1970-01-01 00:00:00 (0) +
        // Timezone offset 05:00:00 (54000000) +
        // 2484 daily cycles (2484 * 86_400_000 = 214617600000) =
        // 214671600000 - Wednesday, 20 October 1976 15:00:00 UTC ie. filename is 19761020 local
        doTestCycleAndResourceNames(BIG_POSITIVE_RELATIVE_EPOCH, RollCycles.DAILY, BIG_POSITIVE_RELATIVE_DAILY_CYCLE_NUMBER, BIG_POSITIVE_RELATIVE_DAILY_FILE_NAME);
        doTestCycleAndResourceNames(BIG_POSITIVE_RELATIVE_EPOCH, RollCycles.HOURLY, BIG_POSITIVE_RELATIVE_HOURLY_CYCLE_NUMBER, BIG_POSITIVE_RELATIVE_HOURLY_FILE_NAME_0);
        doTestCycleAndResourceNames(BIG_POSITIVE_RELATIVE_EPOCH, RollCycles.HOURLY, BIG_POSITIVE_RELATIVE_HOURLY_CYCLE_NUMBER + 15, BIG_POSITIVE_RELATIVE_HOURLY_FILE_NAME_15);
        doTestCycleAndResourceNames(BIG_POSITIVE_RELATIVE_EPOCH, RollCycles.MINUTELY, BIG_POSITIVE_RELATIVE_MINUTELY_CYCLE_NUMBER, BIG_POSITIVE_RELATIVE_MINUTELY_FILE_NAME_0);
        doTestCycleAndResourceNames(BIG_POSITIVE_RELATIVE_EPOCH, RollCycles.MINUTELY, BIG_POSITIVE_RELATIVE_MINUTELY_CYCLE_NUMBER + 10, BIG_POSITIVE_RELATIVE_MINUTELY_FILE_NAME_10);
        doTestCycleAndResourceNames(BIG_POSITIVE_RELATIVE_EPOCH, WeeklyRollCycle.INSTANCE, 354, "1976288");

        // NEGATIVE_RELATIVE_EPOCH is -3 hours (-10800000 millis)
        // cycle 2484 should be formatted as:
        // epoch 1969-12-31 00:00:00 (-86400000) +
        // Timezone offset -03:00:00 (-10800000) +
        // 2484 daily cycles (2484 * 86_400_000 = 214617600000) =
        // 214520400000 - Monday, 18 October 1976 21:00:00 UTC ie. filename is 19761019 local
        doTestCycleAndResourceNames(NEGATIVE_RELATIVE_EPOCH, RollCycles.DAILY, NEGATIVE_RELATIVE_DAILY_CYCLE_NUMBER, NEGATIVE_RELATIVE_DAILY_FILE_NAME);
        doTestCycleAndResourceNames(NEGATIVE_RELATIVE_EPOCH, RollCycles.HOURLY, NEGATIVE_RELATIVE_HOURLY_CYCLE_NUMBER, NEGATIVE_RELATIVE_HOURLY_FILE_NAME_0);
        doTestCycleAndResourceNames(NEGATIVE_RELATIVE_EPOCH, RollCycles.HOURLY, NEGATIVE_RELATIVE_HOURLY_CYCLE_NUMBER + 15, NEGATIVE_RELATIVE_HOURLY_FILE_NAME_15);
        doTestCycleAndResourceNames(NEGATIVE_RELATIVE_EPOCH, RollCycles.MINUTELY, NEGATIVE_RELATIVE_MINUTELY_CYCLE_NUMBER, NEGATIVE_RELATIVE_MINUTELY_FILE_NAME_0);
        doTestCycleAndResourceNames(NEGATIVE_RELATIVE_EPOCH, RollCycles.MINUTELY, NEGATIVE_RELATIVE_MINUTELY_CYCLE_NUMBER + 10, NEGATIVE_RELATIVE_MINUTELY_FILE_NAME_10);
        doTestCycleAndResourceNames(BIG_POSITIVE_RELATIVE_EPOCH, WeeklyRollCycle.INSTANCE, 354, "1976288");

        // NEGATIVE_RELATIVE_EPOCH is -13 hours (-46800000 millis)
        // cycle 2484 should be formatted as:
        // epoch 1969-12-31 00:00:00 (-86400000) +
        // Timezone offset -03:00:00 (-46800000) +
        // 2484 daily cycles (2484 * 86_400_000 = 214617600000) =
        // 214484400000 - Monday, 18 October 1976 11:00:00 UTC ie. filename is 19761019 local
        doTestCycleAndResourceNames(BIG_NEGATIVE_RELATIVE_EPOCH, RollCycles.DAILY, BIG_NEGATIVE_RELATIVE_DAILY_CYCLE_NUMBER, BIG_NEGATIVE_RELATIVE_DAILY_FILE_NAME);
        doTestCycleAndResourceNames(BIG_NEGATIVE_RELATIVE_EPOCH, RollCycles.HOURLY, BIG_NEGATIVE_RELATIVE_HOURLY_CYCLE_NUMBER, BIG_NEGATIVE_RELATIVE_HOURLY_FILE_NAME_0);
        doTestCycleAndResourceNames(BIG_NEGATIVE_RELATIVE_EPOCH, RollCycles.HOURLY, BIG_NEGATIVE_RELATIVE_HOURLY_CYCLE_NUMBER + 15, BIG_NEGATIVE_RELATIVE_HOURLY_FILE_NAME_15);
        doTestCycleAndResourceNames(BIG_NEGATIVE_RELATIVE_EPOCH, RollCycles.MINUTELY, BIG_NEGATIVE_RELATIVE_MINUTELY_CYCLE_NUMBER, BIG_NEGATIVE_RELATIVE_MINUTELY_FILE_NAME_0);
        doTestCycleAndResourceNames(BIG_NEGATIVE_RELATIVE_EPOCH, RollCycles.MINUTELY, BIG_NEGATIVE_RELATIVE_MINUTELY_CYCLE_NUMBER + 10, BIG_NEGATIVE_RELATIVE_MINUTELY_FILE_NAME_10);
        doTestCycleAndResourceNames(BIG_NEGATIVE_RELATIVE_EPOCH, WeeklyRollCycle.INSTANCE, 354, "1976287");
    }

    @Test(expected = RuntimeException.class)
    public void parseIncorrectlyFormattedName() {
        final RollingResourcesCache cache =
                new RollingResourcesCache(RollCycles.HOURLY, PM_EPOCH, File::new, File::getName);
        cache.parseCount("foobar-qux");
    }

    @Test
    public void fuzzyConversionTest() {
        final int maxAddition = (int) ChronoUnit.DECADES.getDuration().toMillis();
        final Random random = new Random(SEED);

        for (int i = 0; i < 1_000; i++) {
            final long epoch = random.nextInt(maxAddition);
            final RollingResourcesCache cache =
                    new RollingResourcesCache(RollCycles.DAILY, epoch, File::new, File::getName);

            for (int j = 0; j < 200; j++) {
                final long offsetMillisFromEpoch =
                        TimeUnit.DAYS.toMillis(random.nextInt(500)) +
                                TimeUnit.HOURS.toMillis(random.nextInt(50)) +
                                TimeUnit.MINUTES.toMillis(random.nextInt(50));

                final long instantAfterEpoch = epoch + offsetMillisFromEpoch;
                final ZoneId zoneId = ZoneId.of("UTC");

                final int cycle = RollCycles.DAILY.current(() -> instantAfterEpoch, epoch);

                final long daysBetweenEpochAndInstant = (instantAfterEpoch - epoch) / ONE_DAY_IN_MILLIS;

                assertThat((long) cycle, is(daysBetweenEpochAndInstant));

                assertThat(((long) cycle) * ONE_DAY_IN_MILLIS,
                        is((long) cycle * RollCycles.DAILY.length()));

                if (LOG_TEST_DEBUG) {
                    System.out.printf("Epoch: %d%n", epoch);
                    System.out.printf("Epoch millis: %d(UTC+%dd), current millis: %d(UTC+%dd)%n",
                            epoch, (epoch / ONE_DAY_IN_MILLIS), instantAfterEpoch,
                            (instantAfterEpoch / ONE_DAY_IN_MILLIS));
                    System.out.printf("Delta days: %d, Delta millis: %d, Delta days in millis: %d%n",
                            daysBetweenEpochAndInstant,
                            instantAfterEpoch - epoch,
                            daysBetweenEpochAndInstant * ONE_DAY_IN_MILLIS);
                    System.out.printf("MillisSinceEpoch: %d%n",
                            offsetMillisFromEpoch);
                    System.out.printf("Resource calc of millisSinceEpoch: %d%n",
                            daysBetweenEpochAndInstant * ONE_DAY_IN_MILLIS);
                }

                long effectiveCycleStartTime = (instantAfterEpoch - epoch) -
                        ((instantAfterEpoch - epoch) % ONE_DAY_IN_MILLIS);

                assertCorrectConversion(cache, cycle,
                        Instant.ofEpochMilli(effectiveCycleStartTime + epoch),
                        DateTimeFormatter.ofPattern("yyyyMMdd").withZone(zoneId));
            }
        }
    }

    @Test
    public void testToLong() {
        doTestToLong(RollCycles.DAILY, AM_EPOCH, 0, Long.valueOf("17633"));
        doTestToLong(RollCycles.HOURLY, AM_EPOCH, 0, Long.valueOf("423192"));
        doTestToLong(RollCycles.MINUTELY, AM_EPOCH, 0, Long.valueOf("25391520"));
        doTestToLong(RollCycles.DAILY, AM_EPOCH, 100, Long.valueOf("17733"));
        doTestToLong(RollCycles.HOURLY, AM_EPOCH, 100, Long.valueOf("423292"));
        doTestToLong(RollCycles.MINUTELY, AM_EPOCH, 100, Long.valueOf("25391620"));
        doTestToLong(WeeklyRollCycle.INSTANCE, AM_EPOCH, 0, Long.valueOf("2519"));

        doTestToLong(RollCycles.DAILY, PM_EPOCH, 0, Long.valueOf("14869"));
        doTestToLong(RollCycles.HOURLY, PM_EPOCH, 0, Long.valueOf("356856"));
        doTestToLong(RollCycles.MINUTELY, PM_EPOCH, 0, Long.valueOf("21411360"));
        doTestToLong(RollCycles.DAILY, PM_EPOCH, 100, Long.valueOf("14969"));
        doTestToLong(RollCycles.HOURLY, PM_EPOCH, 100, Long.valueOf("356956"));
        doTestToLong(RollCycles.MINUTELY, PM_EPOCH, 100, Long.valueOf("21411460"));
        doTestToLong(WeeklyRollCycle.INSTANCE, PM_EPOCH, 0, Long.valueOf("2124"));

        doTestToLong(RollCycles.DAILY, POSITIVE_RELATIVE_EPOCH, 0, Long.valueOf("0"));
        doTestToLong(RollCycles.HOURLY, POSITIVE_RELATIVE_EPOCH, 0, Long.valueOf("0"));
        doTestToLong(RollCycles.MINUTELY, POSITIVE_RELATIVE_EPOCH, 0, Long.valueOf("0"));
        doTestToLong(RollCycles.DAILY, POSITIVE_RELATIVE_EPOCH, 100, Long.valueOf("100"));
        doTestToLong(RollCycles.HOURLY, POSITIVE_RELATIVE_EPOCH, 100, Long.valueOf("100"));
        doTestToLong(RollCycles.MINUTELY, POSITIVE_RELATIVE_EPOCH, 100, Long.valueOf("100"));
        doTestToLong(WeeklyRollCycle.INSTANCE, POSITIVE_RELATIVE_EPOCH, 7, Long.valueOf("7"));

        doTestToLong(RollCycles.DAILY, NEGATIVE_RELATIVE_EPOCH, 0, Long.valueOf("-1"));
        doTestToLong(RollCycles.HOURLY, NEGATIVE_RELATIVE_EPOCH, 0, Long.valueOf("-24"));
        doTestToLong(RollCycles.MINUTELY, NEGATIVE_RELATIVE_EPOCH, 0, Long.valueOf("-1440"));
        doTestToLong(RollCycles.DAILY, NEGATIVE_RELATIVE_EPOCH, 100, Long.valueOf("99"));
        doTestToLong(RollCycles.HOURLY, NEGATIVE_RELATIVE_EPOCH, 100, Long.valueOf("76"));
        doTestToLong(RollCycles.MINUTELY, NEGATIVE_RELATIVE_EPOCH, 100, Long.valueOf("-1340"));
        doTestToLong(WeeklyRollCycle.INSTANCE, NEGATIVE_RELATIVE_EPOCH, 0, Long.valueOf("-1"));

    }

    public void doTestToLong(RollCycle rollCycle, long epoch, long cycle, Long expectedLong) {
        RollingResourcesCache cache =
                new RollingResourcesCache(rollCycle, epoch, File::new, File::getName);

        RollingResourcesCache.Resource resource = cache.resourceFor(cycle);
        assertEquals(expectedLong, cache.toLong(resource.path));
    }


}
