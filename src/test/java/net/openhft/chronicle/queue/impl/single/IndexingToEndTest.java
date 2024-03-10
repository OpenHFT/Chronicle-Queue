package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.TailerDirection;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndexingToEndTest extends IndexingTestCommon {

    @ParameterizedTest
    @MethodSource("tailerDirections")
    void fromStart_noData(TailerDirection tailerDirection) {
        tailer.direction(tailerDirection);
        assertEquals(0, tailer.index());
        tailer.toEnd();
        assertEquals(0, tailer.index());
    }

    @ParameterizedTest
    @MethodSource("tailerDirections")
    void fromStart_manyEntriesSingleCycle(TailerDirection tailerDirection) {
        tailer.direction(tailerDirection);
        assertEquals(0, tailer.index());
        long lastIndexAppended = 0;
        for (int i = 0; i < 1_000; i++) {
            appender.writeText("<test>");
            lastIndexAppended = appender.lastIndexAppended();
        }
        tailer.toEnd();
        assertEquals(expectedIndexAfterToEnd(lastIndexAppended, tailerDirection), tailer.index());
    }

    @ParameterizedTest
    @MethodSource("tailerDirections")
    void fromStart_manyEntriesSingleCycle_idempotent(TailerDirection tailerDirection) {
        tailer.direction(tailerDirection);
        assertEquals(0, tailer.index());
        long lastIndexAppended = 0;
        for (int i = 0; i < 1_000; i++) {
            appender.writeText("<test>");
            lastIndexAppended = appender.lastIndexAppended();
        }

        for (int i = 0; i < 100; i++) {
            tailer.toEnd();
            assertEquals(expectedIndexAfterToEnd(lastIndexAppended, tailerDirection), tailer.index());
        }
    }

    @ParameterizedTest
    @MethodSource("tailerDirections")
    void fromStart_manyEntriesMultiCycle(TailerDirection tailerDirection) {
        long lastIndexAppended = populateQueue(tailerDirection);
        tailer.toEnd();
        assertEquals(2, rollCycle().toCycle(lastIndexAppended));
        assertEquals(expectedIndexAfterToEnd(lastIndexAppended, tailerDirection), tailer.index());
    }

    @ParameterizedTest
    @MethodSource("tailerDirections")
    void fromMiddle_manyEntriesMultiCycle(TailerDirection tailerDirection) {
        long lastIndexAppended = populateQueue(tailerDirection);
        moveToMidpoint(lastIndexAppended);
        tailer.toEnd();
        assertEquals(2, rollCycle().toCycle(lastIndexAppended));
        assertEquals(expectedIndexAfterToEnd(lastIndexAppended, tailerDirection), tailer.index());
    }

    @Test
    void lastIndexAppendedEqualsQueueLastIndex() {
        long lastIndexAppended = 0;
        for (int i = 0; i < 3; i++) {
            appender.writeText("seq_" + i);
            lastIndexAppended = appender.lastIndexAppended();
        }

        assertEquals(lastIndexAppended, queue.lastIndex());
        tailer = queue.createTailer().direction(TailerDirection.BACKWARD);
        tailer.toEnd();
        assertEquals(lastIndexAppended, tailer.index());
    }

    @Test
    void readFromWithinWriteContext() {
        long lastIndexAppended = 0;
        for (int i = 0; i < 3; i++) {
            appender.writeText("seq_" + i);
            lastIndexAppended = appender.lastIndexAppended();
        }
        tailer = queue.createTailer();
        try (DocumentContext writeContext = ThreadLocalAppender.acquireThreadLocalAppender(queue).writingDocument()) {
            writeContext.rollbackOnClose();
            tailer = tailer.direction(TailerDirection.BACKWARD);
            tailer.toEnd();
            readAndExpectIndex(2);
            readAndExpectIndex(1);
            readAndExpectIndex(0);
        }
    }

    private void readAndExpectIndex(long expectedIndex) {
        try (DocumentContext readContext = tailer.readingDocument()) {
            if (readContext.isPresent()) {
                assertEquals(expectedIndex, readContext.index());
                assertEquals("åseq_" + expectedIndex, readContext.wire().bytes().toString());
            }
        }
    }

    @Test
    void tolerateDeletedCycleFile() {
        // Create the initial set of queue files
        long lastIndexAppended = populateQueue(TailerDirection.BACKWARD);

        // Delete middle cycle file
        assertEquals(4, queuePath.listFiles().length); // 3 cycle files, one metadata
        File middleCycleFile = Arrays.stream(queuePath.listFiles()).filter(f -> f.getName().equals("19700101-000001T.cq4")).findFirst().get();
        assertEquals("19700101-000001T.cq4", middleCycleFile.getName()); // Ensure middle cycle, cycles = [0,1,2]
        queue.close(); // Close to ensure deletion succeeds
        middleCycleFile.delete();
        assertEquals(3, queuePath.listFiles().length); // 2 cycle files, one metadata

        try (SingleChronicleQueue queue = createQueueInstance(); ExcerptTailer tailer = queue.createTailer()) {
            tailer.direction(TailerDirection.BACKWARD);
            assertEquals(0, tailer.index());
            tailer.toEnd();
            assertEquals(8589934592L, tailer.index());
            assertEquals(lastIndexAppended, tailer.index());
        }
    }

    @Test
    void tolerateSkippedCycleFile() {
        // Create the initial set of queue files
        appender.writeText("seq_" + 0);
        timeProvider.advanceMillis(2001); // Skip a cycle
        appender.writeText("seq_" + 1);
        long lastIndexAppended = appender.lastIndexAppended();

        // Delete middle cycle file
        Set<String> fileNames = Arrays.stream(queuePath.listFiles()).map(File::getName).collect(Collectors.toSet());
        assertEquals(3, fileNames.size()); // 3 cycle files, one metadata
        assertTrue(fileNames.contains("19700101-000000T.cq4"));
        assertTrue(fileNames.contains("19700101-000002T.cq4"));

        try (SingleChronicleQueue queue = createQueueInstance(); ExcerptTailer tailer = queue.createTailer()) {
            tailer.direction(TailerDirection.BACKWARD);
            assertEquals(0, tailer.index());
            tailer.toEnd();
            assertEquals(8589934592L, tailer.index());
            assertEquals(lastIndexAppended, tailer.index());
        }
    }

    private void moveToMidpoint(long lastIndexAppended) {
        int cycle = queue.rollCycle().toCycle(lastIndexAppended);
        int middleCycle = cycle / 2;
        long desiredIndex = queue.rollCycle().toIndex(middleCycle, 0);
        boolean moveToIndexResult = tailer.moveToIndex(desiredIndex);
        assertTrue(moveToIndexResult);
        assertEquals(desiredIndex, tailer.index());
    }

    private long populateQueue(TailerDirection tailerDirection) {
        tailer.direction(tailerDirection);
        appender.writeText("<init>");
        assertEquals(0, tailer.index());
        long lastIndexAppended = 0;
        for (int i = 0; i < 3; i++) {
            appender.writeText("seq_" + i);
            timeProvider.advanceMillis(1001);
            lastIndexAppended = appender.lastIndexAppended();
        }
        return lastIndexAppended;
    }

    public static Stream<TailerDirection> tailerDirections() {
        return Stream.of(TailerDirection.NONE, TailerDirection.FORWARD, TailerDirection.BACKWARD);
    }

    private long expectedIndexAfterToEnd(long lastIndexAppended, TailerDirection tailerDirection) {
        if (tailerDirection == TailerDirection.BACKWARD) {
            return lastIndexAppended;
        } else {
            return lastIndexAppended + 1;
        }
    }

}
