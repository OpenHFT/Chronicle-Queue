/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single.stress.backwardstailer;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.rollcycles.LargeRollCycles;
import net.openhft.chronicle.queue.rollcycles.LegacyRollCycles;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(BackwardsTailerToEndPerfAcceptanceTest.BackwardsTailerToEndPerfAcceptanceTemplateProvider.class)
public class BackwardsTailerToEndPerfAcceptanceTest extends QueueTestCommon {

    private static final Logger log = LoggerFactory.getLogger(BackwardsTailerToEndPerfAcceptanceTest.class);

    private final RollCycle rollCycle;

    private final TailerIndexStartPosition tailerIndexStartPosition;

    private long baseline;

    public BackwardsTailerToEndPerfAcceptanceTest(RollCycle rollCycle, TailerIndexStartPosition tailerIndexStartPosition) {
        this.rollCycle = rollCycle;
        this.tailerIndexStartPosition = tailerIndexStartPosition;
    }

    private static Stream<BackwardsTailerToEndPerfAcceptanceCase> cases() {
        List<BackwardsTailerToEndPerfAcceptanceCase> data = new ArrayList<>();
        data.add(new BackwardsTailerToEndPerfAcceptanceCase(TestRollCycles.TEST_HOURLY, TailerIndexStartPosition.BEGINNING));

        data.add(new BackwardsTailerToEndPerfAcceptanceCase(LegacyRollCycles.DAILY, TailerIndexStartPosition.BEGINNING));
        data.add(new BackwardsTailerToEndPerfAcceptanceCase(LegacyRollCycles.DAILY, TailerIndexStartPosition.MIDDLE));

        data.add(new BackwardsTailerToEndPerfAcceptanceCase(LargeRollCycles.LARGE_DAILY, TailerIndexStartPosition.BEGINNING));
        data.add(new BackwardsTailerToEndPerfAcceptanceCase(TestRollCycles.TEST2_DAILY, TailerIndexStartPosition.BEGINNING));
        return data.stream();
    }

    private static final class BackwardsTailerToEndPerfAcceptanceCase {
        private final RollCycle rollCycle;
        private final TailerIndexStartPosition tailerIndexStartPosition;

        private BackwardsTailerToEndPerfAcceptanceCase(RollCycle rollCycle, TailerIndexStartPosition tailerIndexStartPosition) {
            this.rollCycle = rollCycle;
            this.tailerIndexStartPosition = tailerIndexStartPosition;
        }
    }

    static final class BackwardsTailerToEndPerfAcceptanceTemplateProvider implements TestTemplateInvocationContextProvider {
        @Override
        public boolean supportsTestTemplate(ExtensionContext context) {
            return true;
        }

        @Override
        public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
            return cases().map(BackwardsTailerToEndPerfAcceptanceInvocationContext::new);
        }
    }

    private static final class BackwardsTailerToEndPerfAcceptanceInvocationContext implements TestTemplateInvocationContext {
        private final BackwardsTailerToEndPerfAcceptanceCase testCase;

        private BackwardsTailerToEndPerfAcceptanceInvocationContext(BackwardsTailerToEndPerfAcceptanceCase testCase) {
            this.testCase = testCase;
        }

        @Override
        public String getDisplayName(int invocationIndex) {
            return "rollCycle=" + testCase.rollCycle + ", start=" + testCase.tailerIndexStartPosition;
        }

        @Override
        public java.util.List<Extension> getAdditionalExtensions() {
            return java.util.Collections.singletonList(new BackwardsTailerToEndPerfAcceptanceParameterResolver(testCase));
        }
    }

    private static final class BackwardsTailerToEndPerfAcceptanceParameterResolver implements ParameterResolver {
        private final BackwardsTailerToEndPerfAcceptanceCase testCase;

        private BackwardsTailerToEndPerfAcceptanceParameterResolver(BackwardsTailerToEndPerfAcceptanceCase testCase) {
            this.testCase = testCase;
        }

        @Override
        public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
            Class<?> type = parameterContext.getParameter().getType();
            return type == RollCycle.class || type == TailerIndexStartPosition.class;
        }

        @Override
        public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
            Class<?> type = parameterContext.getParameter().getType();
            if (type == RollCycle.class) {
                return testCase.rollCycle;
            }
            return testCase.tailerIndexStartPosition;
        }
    }

    @BeforeEach
    public void before() {
        // Capture baseline performance of toEnd
        log.info("rollCycle={}, tailerIndexStartPosition={}", rollCycle, tailerIndexStartPosition);
        log.info("Capturing baseline performance. rollCycle={}", rollCycle);
        baseline = runTest(rollCycle.defaultIndexCount() * rollCycle.defaultIndexSpacing() - 1, TailerDirection.BACKWARD, tailerIndexStartPosition, rollCycle);
        log.info("Baseline performance captured. rollCycle={}", rollCycle);
    }

    @Disabled("Disabled as too flaky for full suite: from beginning")
    @TestTemplate
    @DisplayName("Tailer toEnd from beginning stays within baseline factor")
    public void fromBeginning() {
        long duration = runTest(rollCycle.defaultIndexCount() * rollCycle.defaultIndexSpacing() + 1, TailerDirection.BACKWARD, tailerIndexStartPosition, rollCycle);
        assertReasonablePerformance(duration);
    }

    @Disabled("Disabled as too flaky for full suite: less than boundary")
    @TestTemplate
    @DisplayName("Tailer toEnd from below boundary stays within baseline factor")
    public void lessThanBoundary() {
        long duration = runTest(rollCycle.defaultIndexCount() * rollCycle.defaultIndexSpacing() + 1, TailerDirection.BACKWARD, tailerIndexStartPosition, rollCycle);
        assertReasonablePerformance(duration);
    }

    @Disabled("Disabled as too flaky for full suite: on boundary")
    @TestTemplate
    @DisplayName("Tailer toEnd on boundary stays within baseline factor")
    public void onBoundary() {
        long duration = runTest(rollCycle.defaultIndexCount() * rollCycle.defaultIndexSpacing(), TailerDirection.BACKWARD, tailerIndexStartPosition, rollCycle);
        assertReasonablePerformance(duration);
    }

    @Disabled("Disabled as too flaky for full suite: above boundary")
    @TestTemplate
    @DisplayName("Tailer toEnd above boundary stays within baseline factor")
    public void greaterThanBoundary() {
        long duration = runTest(rollCycle.defaultIndexCount() * rollCycle.defaultIndexSpacing() - 1, TailerDirection.BACKWARD, tailerIndexStartPosition, rollCycle);
        assertReasonablePerformance(duration);
    }

    private void assertReasonablePerformance(long duration) {
        double factor = (double) duration / baseline;
        long baselineUs = baseline / 1000;
        long durationUs = duration / 1000;
        String message = "Performance of this test was " + factor + "x baseline. baseline=" + baselineUs + "us, duration=" + durationUs + "us.";
        log.info(message);
        assertTrue(factor < 10, message);
    }

    private void populateQueue(int entriesToWrite, ExcerptAppender appender) {
        for (int i = 0; i < entriesToWrite; i++) {
            appender.writeText("message_" + i);

            if (rollCycle.equals(TestRollCycles.TEST2_DAILY)) {
                log.info("lastIndexAppended for TEST2_DAILY={}", appender.lastIndexAppended());
            }
        }
    }

    private long runTest(int entriesToWrite, TailerDirection tailerDirection, TailerIndexStartPosition tailerIndexStartPosition, RollCycle rollCycle) {
        @NotNull File path = getTmpDir();
        try (SingleChronicleQueue queue = createQueue(path, rollCycle);
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer().direction(tailerDirection)) {
            populateQueue(entriesToWrite, appender);

            // Move tailer to appropriate position
            switch (tailerIndexStartPosition) {
                case BEGINNING:
                    tailer.moveToIndex(0);
                    break;
                case MIDDLE:
                    boolean result = tailer.moveToIndex(appender.lastIndexAppended() / 2);
                    assertTrue(result, "tailer should move to middle index before toEnd");
                    break;
                default:
                    throw new IllegalStateException("Unsupported tailerIndexStartPosition - " + tailerIndexStartPosition);
            }

            long start = System.nanoTime();
            tailer.toEnd();
            long stop = System.nanoTime();
            return stop - start;

        } finally {
            IOTools.deleteDirWithFiles(path);
        }
    }

    @NotNull
    private SingleChronicleQueue createQueue(File path, RollCycle rollCycle) {
        SetTimeProvider setTimeProvider = new SetTimeProvider();
        return SingleChronicleQueueBuilder.builder().timeProvider(setTimeProvider).path(path).rollCycle(rollCycle).build();
    }

    public enum TailerIndexStartPosition {
        BEGINNING, MIDDLE
    }
}
