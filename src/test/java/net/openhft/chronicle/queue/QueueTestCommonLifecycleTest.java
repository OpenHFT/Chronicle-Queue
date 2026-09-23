/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.AbstractReferenceCounted;
import net.openhft.chronicle.core.onoes.Slf4jExceptionHandler;
import net.openhft.chronicle.core.onoes.ThreadLocalisedExceptionHandler;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;
import static org.junit.platform.launcher.EngineFilter.includeEngines;

class QueueTestCommonLifecycleTest {
    enum Scenario {
        SUCCESS, BODY_FAILURE, SETUP_FAILURE, AFTER_FAILURE, CLEANUP_FAILURE,
        ABORT, LEAK, BODY_AND_LEAK, ABORT_AND_LEAK, WARNING, WARNING_AND_LEAK,
        EARLY_SETUP_FAILURE, MISSING_EXPECTED_EVENT
    }

    private static State state;

    static Stream<Arguments> scenarios() {
        return Stream.of("junit-vintage", "junit-jupiter").flatMap(engine ->
                Arrays.stream(Scenario.values()).map(scenario -> Arguments.of(engine, scenario)));
    }

    @AfterEach
    void releaseSentinel() {
        if (state != null && state.leak != null)
            state.leak.releaseLast();
        Jvm.resetExceptionHandlers();
        AbstractReferenceCounted.disableReferenceTracing();
        SystemTimeProvider.CLOCK = SystemTimeProvider.INSTANCE;
    }

    @ParameterizedTest(name = "{0}: {1}")
    @MethodSource("scenarios")
    void usesEngineOutcomeAndAlwaysCleansUp(String engine, Scenario scenario) {
        state = new State(scenario);
        TestExecutionSummary result = run(engine, "junit-vintage".equals(engine)
                ? VintageTestCase.class : JupiterTestCase.class);
        assertEquals(1, result.getTestsStartedCount());
        assertEquals(1, state.records, "The selected engine must initialise the shared fixture");
        assertEquals(1, state.cleanups, "Cleanup must finish even after an earlier failure");
        assertSame(SystemTimeProvider.INSTANCE, SystemTimeProvider.CLOCK);
        assertSame(Slf4jExceptionHandler.WARN, ThreadLocalisedExceptionHandler.unwrap(Jvm.warn()));

        boolean aborted = scenario == Scenario.ABORT || scenario == Scenario.ABORT_AND_LEAK;
        boolean successfulBeforeChecks = scenario == Scenario.SUCCESS || scenario == Scenario.LEAK ||
                scenario == Scenario.WARNING || scenario == Scenario.WARNING_AND_LEAK ||
                scenario == Scenario.MISSING_EXPECTED_EVENT;
        boolean checkedResources = successfulBeforeChecks && scenario != Scenario.WARNING &&
                scenario != Scenario.WARNING_AND_LEAK && scenario != Scenario.MISSING_EXPECTED_EVENT;
        assertEquals(checkedResources ? 1 : 0, state.referenceChecks);

        if (aborted) {
            assertEquals(1, result.getTestsAbortedCount());
            assertEquals(0, result.getTestsFailedCount());
        } else if (scenario == Scenario.SUCCESS) {
            assertEquals(1, result.getTestsSucceededCount(), result.getFailures().toString());
        } else {
            assertEquals(1, result.getTestsFailedCount(), result.getFailures().toString());
            Throwable failure = result.getFailures().get(0).getException();
            if (!successfulBeforeChecks)
                assertSame(state.primary, failure);
            else if (scenario == Scenario.LEAK)
                assertNotNull(state.resourceFailure, "A passing body with a real leak must fail");
            else
                assertTrue(failure.toString().contains("fixture sentinel"), failure.toString());
        }
    }

    @Test
    void collectedFailureIsObservedOutsideTheCollector() {
        state = new State(Scenario.SUCCESS);
        TestExecutionSummary result = run("junit-vintage", CollectedFailureTestCase.class);
        assertEquals(1, result.getTestsFailedCount());
        assertSame(state.primary, result.getFailures().get(0).getException());
        assertEquals(0, state.referenceChecks);
        assertEquals(1, state.cleanups);
    }

    @Test
    void expectedExceptionStillRequiresCleanResources() {
        state = new State(Scenario.LEAK);
        TestExecutionSummary result = run("junit-vintage", ExpectedExceptionTestCase.class);
        assertEquals(1, result.getTestsFailedCount());
        assertNotNull(state.resourceFailure);
        assertEquals(1, state.referenceChecks);
    }

    @Test
    void timeoutSkipsResourceChecks() {
        state = new State(Scenario.SUCCESS);
        TestExecutionSummary result = run("junit-vintage", TimeoutTestCase.class);
        assertEquals(1, result.getTestsFailedCount());
        assertTrue(result.getFailures().get(0).getException() instanceof org.junit.runners.model.TestTimedOutException);
        assertEquals(0, state.referenceChecks);
        assertEquals(1, state.cleanups);
    }

    @Test
    void enclosingJupiterFixtureWorksForNestedTests() {
        state = new State(Scenario.SUCCESS);
        TestExecutionSummary result = run("junit-jupiter", NestedTestCase.class);
        assertEquals(1, result.getTestsSucceededCount(), result.getFailures().toString());
        assertEquals(1, state.records);
        assertEquals(1, state.referenceChecks);
        assertEquals(1, state.cleanups);
    }

    @Test
    void followingTestDoesNotInheritFailureOrExpectedEvents() {
        state = new State(Scenario.BODY_FAILURE);
        assertEquals(1, run("junit-jupiter", JupiterTestCase.class).getTestsFailedCount());
        state = new State(Scenario.SUCCESS);
        assertEquals(1, run("junit-jupiter", JupiterTestCase.class).getTestsSucceededCount());
        assertEquals(1, state.referenceChecks);
    }

    private static TestExecutionSummary run(String engine, Class<?> fixture) {
        SummaryGeneratingListener listener = new SummaryGeneratingListener();
        LauncherFactory.create().execute(LauncherDiscoveryRequestBuilder.request()
                .selectors(selectClass(fixture)).filters(includeEngines(engine))
                .configurationParameter("junit.jupiter.execution.parallel.enabled", "false").build(), listener);
        return listener.getSummary();
    }

    private static final class State {
        final Scenario scenario;
        final AssertionError primary = new AssertionError("original fixture failure");
        int records;
        int cleanups;
        int referenceChecks;
        Bytes<?> leak;
        Throwable resourceFailure;

        State(Scenario scenario) {
            this.scenario = scenario;
        }
    }

    public abstract static class Fixture extends QueueTestCommon {
        void setupCase() {
            SystemTimeProvider.CLOCK = new SetTimeProvider();
            if (state.scenario == Scenario.SETUP_FAILURE)
                throw state.primary;
            if (state.scenario == Scenario.MISSING_EXPECTED_EVENT || state.scenario == Scenario.BODY_FAILURE)
                expectException("fixture sentinel");
        }

        void bodyCase() {
            if (state.scenario == Scenario.LEAK || state.scenario == Scenario.BODY_AND_LEAK ||
                    state.scenario == Scenario.ABORT_AND_LEAK || state.scenario == Scenario.WARNING_AND_LEAK)
                state.leak = Bytes.allocateDirect(8);
            if (state.scenario == Scenario.BODY_FAILURE || state.scenario == Scenario.BODY_AND_LEAK)
                throw state.primary;
            if (state.scenario == Scenario.WARNING || state.scenario == Scenario.WARNING_AND_LEAK)
                Jvm.warn().on(getClass(), "fixture sentinel");
        }

        void afterCase() {
            if (state.scenario == Scenario.AFTER_FAILURE)
                throw state.primary;
        }

        @Override public void recordExceptions() {
            state.records++;
            if (state.scenario == Scenario.EARLY_SETUP_FAILURE)
                throw state.primary;
            super.recordExceptions();
        }

        @Override protected void preAfter() {
            if (state.scenario == Scenario.CLEANUP_FAILURE)
                throw state.primary;
        }

        @Override protected void tearDown() {
            state.cleanups++;
            super.tearDown();
        }

        @Override public void assertReferencesReleased() {
            state.referenceChecks++;
            try {
                super.assertReferencesReleased();
            } catch (Throwable failure) {
                state.resourceFailure = failure;
                throw failure;
            }
        }
    }

    public static class VintageTestCase extends Fixture {
        @org.junit.Before public void setup() { setupCase(); }
        @org.junit.After public void after() { afterCase(); }
        @org.junit.Test public void body() {
            bodyCase();
            org.junit.Assume.assumeFalse(state.scenario == Scenario.ABORT || state.scenario == Scenario.ABORT_AND_LEAK);
        }
    }

    public static class JupiterTestCase extends Fixture {
        @BeforeEach public void setup() { setupCase(); }
        @AfterEach public void after() { afterCase(); }
        @Test public void body() {
            bodyCase();
            org.junit.jupiter.api.Assumptions.assumeFalse(state.scenario == Scenario.ABORT || state.scenario == Scenario.ABORT_AND_LEAK);
        }
    }

    public static class CollectedFailureTestCase extends Fixture {
        @org.junit.Test public void body() { errorCollector.addError(state.primary); }
    }

    public static class ExpectedExceptionTestCase extends Fixture {
        @org.junit.Test(expected = IllegalArgumentException.class)
        public void body() {
            bodyCase();
            throw new IllegalArgumentException("expected rejection");
        }
    }

    public static class TimeoutTestCase extends Fixture {
        @org.junit.Test(timeout = 100)
        public void body() throws InterruptedException { new CountDownLatch(1).await(); }
    }

    public static class NestedTestCase extends Fixture {
        @org.junit.jupiter.api.Nested
        class InnerTestCase {
            @Test void body() { bodyCase(); }
        }
    }
}
