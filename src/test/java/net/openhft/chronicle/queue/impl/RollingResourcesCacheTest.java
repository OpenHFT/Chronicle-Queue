package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.queue.RollCycles;
import org.junit.Test;

import java.io.File;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class RollingResourcesCacheTest {
    private static final long SEED = 2983472039423847L;
    // Friday, 17 September 2010 16:00:00
    private static final long BUGGY_EPOCH = 1284739200000L;
    private static final String FILE_NAME = "19761020";
    private static final int CYCLE_NUMBER = 2484;
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final RollCycles ROLL_CYCLE = RollCycles.DAILY;
    private static final long ONE_DAY_IN_MILLIS = TimeUnit.DAYS.toMillis(1L);
    private static final boolean LOG_TEST_DEBUG =
                Boolean.valueOf(RollingResourcesCacheTest.class.getSimpleName() + ".debug");

    @Test
    public void shouldConvertCyclesToResourceNamesWithNoEpoch() throws Exception {
        final int epoch = 0;
        final RollingResourcesCache cache =
                new RollingResourcesCache(ROLL_CYCLE, epoch, File::new, File::getName);

        final int cycle = ROLL_CYCLE.current(System::currentTimeMillis, 0);
        assertCorrectConversion(cache, cycle, Instant.now(),
                DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneId.systemDefault()));
    }

    /*
        Epoch is after midday, which means that the ZoneOffset calculated is negative.
        This generates file names that are off-by-one for a cycle if not corrected.
     */
    @Test
    public void shouldCorrectlyConvertCyclesToResourceNamesWithEpoch() throws Exception {
        final RollingResourcesCache cache =
                new RollingResourcesCache(ROLL_CYCLE, BUGGY_EPOCH, File::new, File::getName);
        // cycle zero should be formatted as 1970-01-01
        // cycle 2484 should be formatted as 1970-01-01(0) + (2484 * 86_400_000) = 214617600 =
        // Wednesday, 20 October 1976 00:00:00

        assertThat(cache.resourceFor(CYCLE_NUMBER).text, is(FILE_NAME));
        assertThat(cache.parseCount(FILE_NAME), is(CYCLE_NUMBER));
    }

    @Test
    public void fuzzyConversionTest() throws Exception {
        final int maxAddition = (int) ChronoUnit.DECADES.getDuration().toMillis();
        final Random random = new Random(SEED);

        for (int i = 0; i < 1_000; i++) {
            final long epoch = random.nextInt(maxAddition);
            final RollingResourcesCache cache =
                    new RollingResourcesCache(ROLL_CYCLE, epoch, File::new, File::getName);

            for (int j = 0; j < 200; j++) {
                final long offsetMillisFromEpoch =
                        TimeUnit.DAYS.toMillis(random.nextInt(500)) +
                                TimeUnit.HOURS.toMillis(random.nextInt(50)) +
                                TimeUnit.MINUTES.toMillis(random.nextInt(50));

                final long instantAfterEpoch = epoch + offsetMillisFromEpoch;
                final long moduloMillis = epoch % ONE_DAY_IN_MILLIS;
                final boolean adjustForNegativeOffset = Math.abs(moduloMillis) > ONE_DAY_IN_MILLIS / 2;
                final long offsetMillis;
                if (moduloMillis < ONE_DAY_IN_MILLIS / 2) {
                    offsetMillis = moduloMillis;
                } else {
                    offsetMillis = -(ONE_DAY_IN_MILLIS - moduloMillis);
                }
                final int offsetSeconds = (int) (offsetMillis / 1000);
                final ZoneOffset offset = ZoneOffset.ofTotalSeconds(offsetSeconds);
                final ZoneId zoneId = ZoneId.ofOffset("GMT", offset);

                final int cycle = ROLL_CYCLE.current(() -> instantAfterEpoch, epoch);

                final long daysBetweenEpochAndInstant = (instantAfterEpoch - epoch) / ONE_DAY_IN_MILLIS;

                assertThat((long) cycle, is(daysBetweenEpochAndInstant));

                assertThat(((long) cycle) * ONE_DAY_IN_MILLIS,
                        is((long) cycle * ROLL_CYCLE.length()));

                if (LOG_TEST_DEBUG) {
                    System.out.printf("Epoch: %d%n", epoch);
                    System.out.printf("Offset seconds: %d, zone: %s%n", offsetSeconds, zoneId);
                    System.out.printf("Epoch millis: %d(UTC+%dd), current millis: %d(UTC+%dd)%n",
                            epoch, (epoch / ONE_DAY_IN_MILLIS), instantAfterEpoch,
                            (instantAfterEpoch / ONE_DAY_IN_MILLIS));
                    System.out.printf("Epoch date: %s, Current date: %s, " +
                                    "Delta days: %d, Delta millis: %d, Delta days in millis: %d%n",
                            FORMATTER.format(Instant.ofEpochMilli(epoch).atOffset(offset)),
                            FORMATTER.format(Instant.ofEpochMilli(instantAfterEpoch).atOffset(offset)),
                            daysBetweenEpochAndInstant,
                            instantAfterEpoch - epoch,
                            daysBetweenEpochAndInstant * ONE_DAY_IN_MILLIS);
                    System.out.printf("Effective date relative to epoch: %s, millisSinceEpoch: %d%n",
                            FORMATTER.format(Instant.ofEpochMilli(instantAfterEpoch - epoch).atOffset(offset)),
                            offsetMillisFromEpoch);
                    System.out.printf("Resource calc of millisSinceEpoch: %d%n",
                            daysBetweenEpochAndInstant * ONE_DAY_IN_MILLIS);
                }

                long effectiveCycleStartTime = (instantAfterEpoch - epoch) -
                        ((instantAfterEpoch - epoch) % ONE_DAY_IN_MILLIS);
                if (adjustForNegativeOffset) {
                    effectiveCycleStartTime += ONE_DAY_IN_MILLIS;
                }

                assertCorrectConversion(cache, cycle,
                        Instant.ofEpochMilli(effectiveCycleStartTime),
                        DateTimeFormatter.ofPattern("yyyyMMdd").withZone(zoneId));
            }
        }
    }

    private static void assertCorrectConversion(final RollingResourcesCache cache, final int cycle,
                                                final Instant instant, final DateTimeFormatter formatter) {
        final String expectedFileName = formatter.format(instant);
        assertThat(cache.resourceFor(cycle).text, is(expectedFileName));
        assertThat(cache.parseCount(expectedFileName), is(cycle));
    }
}