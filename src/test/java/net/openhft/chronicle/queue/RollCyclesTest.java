package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.time.TimeProvider;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
@RequiredForClient
public class RollCyclesTest {
    private static final long NO_EPOCH_OFFSET = 0L;
    private static final long SOME_EPOCH_OFFSET = 17L * 37L;
    private static final RollCycles[] TEST_DATA = RollCycles.values();

    private final RollCycles cycle;
    private final AtomicLong clock = new AtomicLong();
    private final TimeProvider timeProvider = clock::get;

    public RollCyclesTest(final String cycleName, final RollCycles cycle) {
        this.cycle = cycle;
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
        final long currentTimeAtStartOfCycle = currentTime - (currentTime % cycle.length());
        clock.set(currentTimeAtStartOfCycle);

        final int startCycle = cycle.current(timeProvider, epochOffset);

        clock.addAndGet(cycle.length());

        assertThat(cycle.current(timeProvider, epochOffset), is(startCycle + 1));
        assertThat(cycle.current(plusOneMillisecond(timeProvider), epochOffset), is(startCycle + 1));
        assertThat(cycle.current(minusOneMillisecond(timeProvider), epochOffset), is(startCycle));

        clock.addAndGet(cycle.length());

        assertThat(cycle.current(timeProvider, epochOffset), is(startCycle + 2));
        assertThat(cycle.current(plusOneMillisecond(timeProvider), epochOffset), is(startCycle + 2));
        assertThat(cycle.current(minusOneMillisecond(timeProvider), epochOffset), is(startCycle + 1));
    }
}
