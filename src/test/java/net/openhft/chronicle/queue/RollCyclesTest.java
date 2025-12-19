/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.time.TimeProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@RequiredForClient
@ExtendWith(RollCyclesTest.RollCyclesTemplateProvider.class)
public class RollCyclesTest extends QueueTestCommon {
    private static final long NO_EPOCH_OFFSET = 0L;
    private static final long SOME_EPOCH_OFFSET = 17L * 37L;
    private static List<Instant> incrementalTimes;

    private final RollCycle cycle;
    private final AtomicLong clock = new AtomicLong();
    private final TimeProvider timeProvider = clock::get;

    public RollCyclesTest(final String cycleName, final RollCycle cycle) {
        this.cycle = cycle;
    }

    private static Stream<RollCyclesCase> cases() {
        return StreamSupport.stream(RollCycles.all().spliterator(), false)
                .map(cycle -> new RollCyclesCase(((Enum<?>) cycle).name(), cycle));
    }

    private static final class RollCyclesCase {
        private final String cycleName;
        private final RollCycle cycle;

        private RollCyclesCase(String cycleName, RollCycle cycle) {
            this.cycleName = cycleName;
            this.cycle = cycle;
        }
    }

    static final class RollCyclesTemplateProvider implements TestTemplateInvocationContextProvider {
        @Override
        public boolean supportsTestTemplate(ExtensionContext context) {
            return true;
        }

        @Override
        public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
            return cases().map(RollCyclesInvocationContext::new);
        }
    }

    private static final class RollCyclesInvocationContext implements TestTemplateInvocationContext {
        private final RollCyclesCase testCase;

        private RollCyclesInvocationContext(RollCyclesCase testCase) {
            this.testCase = testCase;
        }

        @Override
        public String getDisplayName(int invocationIndex) {
            return testCase.cycleName;
        }

        @Override
        public java.util.List<Extension> getAdditionalExtensions() {
            return Collections.singletonList(new RollCyclesParameterResolver(testCase));
        }
    }

    private static final class RollCyclesParameterResolver implements ParameterResolver {
        private final RollCyclesCase testCase;

        private RollCyclesParameterResolver(RollCyclesCase testCase) {
            this.testCase = testCase;
        }

        @Override
        public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
            Class<?> type = parameterContext.getParameter().getType();
            return type == String.class || type == RollCycle.class;
        }

        @Override
        public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
            Class<?> type = parameterContext.getParameter().getType();
            if (type == String.class) {
                return testCase.cycleName;
            }
            return testCase.cycle;
        }
    }

    @BeforeAll
    public static void generateTimes() {
        incrementalTimes = generateIncrementalTimes();
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

    @TestTemplate
    public void shouldBe32bitShifted() {
        long factor = (long) cycle.defaultIndexCount() * cycle.defaultIndexCount() * cycle.defaultIndexSpacing();
        if (factor < 1L << 32)
            factor = 1L << 32;
        assertEquals(factor, cycle.toIndex(1, 0), "cycle.toIndex(1, 0)");
    }

    @TestTemplate
    public void shouldDetermineCurrentCycle() {
        assertCycleRollTimes(NO_EPOCH_OFFSET, withDelta(timeProvider, NO_EPOCH_OFFSET));
    }

    @TestTemplate
    public void shouldTakeEpochIntoAccoutWhenCalculatingCurrentCycle() {
        assertCycleRollTimes(SOME_EPOCH_OFFSET, withDelta(timeProvider, SOME_EPOCH_OFFSET));
    }

    @TestTemplate
    public void shouldHandleReasonableDateRange() {
        final int currentCycle = DefaultCycleCalculator.INSTANCE.currentCycle(cycle, timeProvider, 0);
        // ~ 14 Jul 2017 to 18 May 2033
        for (long nowMillis = 1_500_000_000_000L; nowMillis < 2_000_000_000_000L; nowMillis += (long) 3e10) {
            clock.set(nowMillis);
            long index = cycle.toIndex(currentCycle, 0);
            assertEquals(currentCycle, cycle.toCycle(index), "cycle.toCycle(index)");
        }
    }

    private void assertCycleRollTimes(final long epochOffset, final TimeProvider timeProvider) {
        final long currentTime = System.currentTimeMillis();
        final long currentTimeAtStartOfCycle = currentTime - (currentTime % cycle.lengthInMillis());
        clock.set(currentTimeAtStartOfCycle);

        final int startCycle = cycle.current(timeProvider, epochOffset);

        clock.addAndGet(cycle.lengthInMillis());

        assertEquals(startCycle + 1, cycle.current(timeProvider, epochOffset), "cycle.current(timeProvider, epochOffset)");
        assertEquals(startCycle + 1, cycle.current(plusOneMillisecond(timeProvider), epochOffset), "cycle.current(plusOneMillisecond(timeProvider), epochOffset)");
        assertEquals(startCycle, cycle.current(minusOneMillisecond(timeProvider), epochOffset), "cycle.current(minusOneMillisecond(timeProvider), epochOffset)");

        clock.addAndGet(cycle.lengthInMillis());

        assertEquals(startCycle + 2, cycle.current(timeProvider, epochOffset), "cycle.current(timeProvider, epochOffset)");
        assertEquals(startCycle + 2, cycle.current(plusOneMillisecond(timeProvider), epochOffset), "cycle.current(plusOneMillisecond(timeProvider), epochOffset)");
        assertEquals(startCycle + 1, cycle.current(minusOneMillisecond(timeProvider), epochOffset), "cycle.current(minusOneMillisecond(timeProvider), epochOffset)");
    }

    @TestTemplate
    public void lexicographicOrderShouldCorrelateToChronologicalOrder() {
        String lastName = null;
        Instant lastDate = null;
        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(cycle.format()).withZone(ZoneId.of("UTC"));
        for (final Instant currentDate : incrementalTimes) {
            String currentName = formatter.format(currentDate);
            if (lastName != null) {
                if (lastName.compareTo(currentName) > 0) {
                    fail(format("RollCycle.%s name for %s is lexicographically greater than that for %s, this breaks the contract (%s > %s)",
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
