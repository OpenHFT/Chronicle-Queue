package net.openhft.chronicle.queue.impl.single.stress.backwardstailer;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class BackwardsTailerToEndPerfAcceptanceTest extends ChronicleQueueTestBase {

    private static final Logger log = LoggerFactory.getLogger(BackwardsTailerToEndPerfAcceptanceTest.class);

    private final RollCycle rollCycle;

    private final TailerIndexStartPosition tailerIndexStartPosition;

    private long baseline;

    public BackwardsTailerToEndPerfAcceptanceTest(RollCycle rollCycle, TailerIndexStartPosition tailerIndexStartPosition) {
        this.rollCycle = rollCycle;
        this.tailerIndexStartPosition = tailerIndexStartPosition;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        final List<Object[]> data = new ArrayList<>();
        data.add(new Object[]{RollCycles.TEST_HOURLY, TailerIndexStartPosition.BEGINNING});

        data.add(new Object[]{RollCycles.DAILY, TailerIndexStartPosition.BEGINNING});
        data.add(new Object[]{RollCycles.DAILY, TailerIndexStartPosition.MIDDLE});

        data.add(new Object[]{RollCycles.LARGE_DAILY, TailerIndexStartPosition.BEGINNING});
        data.add(new Object[]{RollCycles.TEST2_DAILY, TailerIndexStartPosition.BEGINNING});
        return data;
    }

    @Before
    public void before() {
        // Capture baseline performance of toEnd
        log.info("rollCycle={}, tailerIndexStartPosition={}", rollCycle, tailerIndexStartPosition);
        log.info("Capturing baseline performance. rollCycle={}", rollCycle);
        baseline = runTest(rollCycle.defaultIndexCount() * rollCycle.defaultIndexSpacing() - 1, TailerDirection.BACKWARD, tailerIndexStartPosition, rollCycle);
        log.info("Baseline performance captured. rollCycle={}", rollCycle);
    }

    @Ignore("Disabled as too flaky when run as part of the full test suite")
    @Test
    public void fromBeginning() {
        long duration = runTest(rollCycle.defaultIndexCount() * rollCycle.defaultIndexSpacing() + 1, TailerDirection.BACKWARD, tailerIndexStartPosition, rollCycle);
        assertReasonablePerformance(duration);
    }

    @Ignore("Disabled as too flaky when run as part of the full test suite")
    @Test
    public void lessThanBoundary() {
        long duration = runTest(rollCycle.defaultIndexCount() * rollCycle.defaultIndexSpacing() + 1, TailerDirection.BACKWARD, tailerIndexStartPosition, rollCycle);
        assertReasonablePerformance(duration);
    }

    @Ignore("Disabled as too flaky when run as part of the full test suite")
    @Test
    public void onBoundary() {
        long duration = runTest(rollCycle.defaultIndexCount() * rollCycle.defaultIndexSpacing(), TailerDirection.BACKWARD, tailerIndexStartPosition, rollCycle);
        assertReasonablePerformance(duration);
    }

    @Ignore("Disabled as too flaky when run as part of the full test suite")
    @Test
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
        assertTrue(message, factor < 10);
    }

    private void populateQueue(int entriesToWrite, ExcerptAppender appender) {
        for (int i = 0; i < entriesToWrite; i++) {
            appender.writeText("message_" + i);

            if (rollCycle.equals(RollCycles.TEST2_DAILY)) {
                log.info("lastIndexAppended={}", appender.lastIndexAppended());
            }

        }
    }

    private long runTest(int entriesToWrite, TailerDirection tailerDirection, TailerIndexStartPosition tailerIndexStartPosition, RollCycle rollCycle) {
        @NotNull File path = getTmpDir();
        try (SingleChronicleQueue queue = createQueue(path, rollCycle);
             ExcerptAppender appender = queue.acquireAppender();
             ExcerptTailer tailer = queue.createTailer().direction(tailerDirection)) {
            populateQueue(entriesToWrite, appender);

            // Move tailer to appropriate position
            switch (tailerIndexStartPosition) {
                case BEGINNING:
                    tailer.moveToIndex(0);
                    break;
                case MIDDLE:
                    boolean result = tailer.moveToIndex(appender.lastIndexAppended() / 2);
                    assertTrue(result);
                    break;
                default:
                    throw new IllegalStateException("Unsupported tailerIndexStartPosition - " + tailerIndexStartPosition);
            }

            long start = System.nanoTime();
            tailer.toEnd();
            long stop = System.nanoTime();
            long elapsed = stop - start;
            return elapsed;

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
