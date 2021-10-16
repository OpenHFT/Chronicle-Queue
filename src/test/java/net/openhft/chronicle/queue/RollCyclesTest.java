package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.time.TimeProvider;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@RequiredForClient
public class RollCyclesTest extends QueueTestCommon {
    private static final long NO_EPOCH_OFFSET = 0L;
    private static final long SOME_EPOCH_OFFSET = 17L * 37L;
    private static final RollCycles[] TEST_DATA = RollCycles.values();
    private static List<Instant> incrementalTimes;

    private final RollCycles cycle;
    private final AtomicLong clock = new AtomicLong();
    private final TimeProvider timeProvider = clock::get;

    public RollCyclesTest(final String cycleName, final RollCycles cycle) {
        this.cycle = cycle;
    }

    @BeforeClass
    public static void generateTimes() {
        incrementalTimes = generateIncrementalTimes();
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        final List<Object[]> data = new ArrayList<>();
        for (RollCycles testDatum : TEST_DATA) {
            data.add(new Object[]{testDatum.name(), testDatum});
        }
        return data;
    }

    private static TimeProvider withDelta(final TimeProvider delegate, final long deltaMillis) {
        return () -> delegate.currentTimeMillis() + deltaMillis;
    }

    private static TimeProvider plusOneMillisecond(final TimeProvider delegate) {
        return () -> delegate.currentTimeMillis() + 1;
    }

    private static TimeProvider minusOneMillisecond(final TimeProvider delegate) {
        return () -> delegate.currentTimeMillis() - 1;
    }

    @Test
    public void shouldBe32bitShifted() {
        long factor = (long) cycle.defaultIndexCount() * cycle.defaultIndexCount() * cycle.defaultIndexSpacing();
        if (factor < 1L << 32)
            factor = 1L << 32;
        assertEquals(factor, cycle.toIndex(1, 0));
    }

    @Test
    public void shouldDetermineCurrentCycle() {
        assertCycleRollTimes(NO_EPOCH_OFFSET, withDelta(timeProvider, NO_EPOCH_OFFSET));
    }

    @Test
    public void shouldTakeEpochIntoAccoutWhenCalculatingCurrentCycle() {
        assertCycleRollTimes(SOME_EPOCH_OFFSET, withDelta(timeProvider, SOME_EPOCH_OFFSET));
    }

    @Test
    public void shouldHandleReasonableDateRange() {
        final int currentCycle = DefaultCycleCalculator.INSTANCE.currentCycle(cycle, timeProvider, 0);
        // ~ 14 Jul 2017 to 18 May 2033
        for (long nowMillis = 1_500_000_000_000L; nowMillis < 2_000_000_000_000L; nowMillis += 3e10) {
            clock.set(nowMillis);
            long index = cycle.toIndex(currentCycle, 0);
            assertEquals(currentCycle, cycle.toCycle(index));
        }
    }

    private void assertCycleRollTimes(final long epochOffset, final TimeProvider timeProvider) {
        final long currentTime = System.currentTimeMillis();
        final long currentTimeAtStartOfCycle = currentTime - (currentTime % cycle.lengthInMillis());
        clock.set(currentTimeAtStartOfCycle);

        final int startCycle = cycle.current(timeProvider, epochOffset);

        clock.addAndGet(cycle.lengthInMillis());

        assertEquals(startCycle + 1, cycle.current(timeProvider, epochOffset));
        assertEquals(startCycle + 1, cycle.current(plusOneMillisecond(timeProvider), epochOffset));
        assertEquals(startCycle, cycle.current(minusOneMillisecond(timeProvider), epochOffset));

        clock.addAndGet(cycle.lengthInMillis());

        assertEquals(startCycle + 2, cycle.current(timeProvider, epochOffset));
        assertEquals(startCycle + 2, cycle.current(plusOneMillisecond(timeProvider), epochOffset));
        assertEquals(startCycle + 1, cycle.current(minusOneMillisecond(timeProvider), epochOffset));
    }

    @Test
    public void lexicographicOrderShouldCorrelateToChronologicalOrder() {
        String lastName = null;
        Instant lastDate = null;
        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(cycle.format()).withZone(ZoneId.of("UTC"));
        for (final Instant currentDate : incrementalTimes) {
            String currentName = formatter.format(currentDate);
            if (lastName != null) {
                if (lastName.compareTo(currentName) > 0) {
                    throw new AssertionError(format("RollCycle.%s name for %s is lexicographically greater than that for %s, this breaks the contract (%s > %s)",
                            cycle, lastDate, currentDate, lastName, currentName));
                }
            }
            lastName = currentName;
            lastDate = currentDate;
        }
    }

    /**
     * Generates a chronologically ordered list of {@link Instant}s
     * that should exercise all {@link RollCycle} patterns
     *
     * @return The {@link Instant}s
     */
    private static List<Instant> generateIncrementalTimes() {
        List<Instant> times = new ArrayList<>();
        Instant currentTime = Instant.ofEpochMilli(1634334361895L);
        times.add(currentTime);
        // first 60 seconds (2 second intervals)
        currentTime = addNext(times, currentTime, 30, ChronoUnit.SECONDS, 2);
        // next 90 minutes (2 minute intervals)
        currentTime = addNext(times, currentTime, 45, ChronoUnit.MINUTES, 2);
        // next 48 hours (2 hour intervals)
        currentTime = addNext(times, currentTime, 24, ChronoUnit.HOURS, 2);
        // next 400 days (4-day intervals)
        addNext(times, currentTime, 100, ChronoUnit.DAYS, 4);
        return times;
    }

    private static Instant addNext(List<Instant> allTimes, Instant startTime, int count, TemporalUnit unit, int stride) {
        for (int i = 0; i < count; i++) {
            startTime = startTime.plus(stride, unit);
            allTimes.add(startTime);
        }
        return startTime;
    }
}
