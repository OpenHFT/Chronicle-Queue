package net.openhft.chronicle.queue.impl.single.stress.backwardstailer;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
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

    private long baseline;

    public BackwardsTailerToEndPerfAcceptanceTest(RollCycle rollCycle) {
        this.rollCycle = rollCycle;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        final List<Object[]> data = new ArrayList<>();
        for (RollCycles rollCycle : new RollCycles[]{RollCycles.HOURLY, RollCycles.DAILY, RollCycles.LARGE_DAILY, RollCycles.TEST2_DAILY}) {
            data.add(new Object[]{rollCycle});
        }
        return data;
    }

    @Before
    public void before() {
        // Capture baseline performance of toEnd
        baseline = runTest(rollCycle.defaultIndexCount() * rollCycle.defaultIndexSpacing() - 1, TailerDirection.BACKWARD, rollCycle);
    }

    @Test
    public void onBoundary() {
        long duration = runTest(rollCycle.defaultIndexCount() * rollCycle.defaultIndexSpacing(), TailerDirection.BACKWARD, rollCycle);
        assertReasonablePerformance(duration);
    }

    @Test
    public void lessThanBoundary() {
        long duration = runTest(rollCycle.defaultIndexCount() * rollCycle.defaultIndexSpacing() + 1, TailerDirection.BACKWARD, rollCycle);
        assertReasonablePerformance(duration);
    }

    @Test
    public void greaterThanBoundary() {
        long duration = runTest(rollCycle.defaultIndexCount() * rollCycle.defaultIndexSpacing() - 1, TailerDirection.BACKWARD, rollCycle);
        assertReasonablePerformance(duration);
    }

    private void assertReasonablePerformance(long duration) {
        double factor = (double) duration / baseline;
        long baselineUs = baseline / 1000;
        long durationUs = duration / 1000;
        String message = "Performance of this test was " + factor + "x baseline. baseline=" + baselineUs + "us, duration=" + durationUs + "us.";
        log.info(message);
        assertTrue(message, factor < 3);
    }

    private void populateQueue(int entriesToWrite, ExcerptAppender appender) {
        for (int i = 0; i < entriesToWrite; i++) {
            appender.writeText("message_" + i);
        }
    }

    private long runTest(int entriesToWrite,
                         TailerDirection tailerDirection, RollCycle rollCycle) {
        @NotNull File path = getTmpDir();
        try (SingleChronicleQueue queue = createQueue(path, rollCycle);
             ExcerptAppender appender = queue.acquireAppender()) {
            populateQueue(entriesToWrite, appender);

            try (ExcerptTailer tailer = queue.createTailer().direction(tailerDirection)) {
                long start = System.nanoTime();
                tailer.toEnd();
                long stop = System.nanoTime();
                long elapsed = stop - start;
                return elapsed;
            }

        } finally {
            IOTools.deleteDirWithFiles(path);
        }
    }

    @NotNull
    private SingleChronicleQueue createQueue(File path,
                                             RollCycle rollCycle) {
        SetTimeProvider setTimeProvider = new SetTimeProvider();
        return SingleChronicleQueueBuilder
                .builder()
                .timeProvider(setTimeProvider)
                .path(path)
                .rollCycle(rollCycle)
                .build();
    }

}
