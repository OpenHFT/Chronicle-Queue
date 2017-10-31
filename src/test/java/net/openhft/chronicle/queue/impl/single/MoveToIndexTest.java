package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.junit.Assert.assertEquals;

public final class MoveToIndexTest {
    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Test
    public void shouldMoveToPreviousIndexAfterDocumentIsConsumed() throws Exception {
        File queuePath = tmpFolder.newFolder("cq");

        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(queuePath).build()) {
            ExcerptAppender appender = queue.acquireAppender();
            for (int i = 1; i < 10; ++i) {
                appender.writeText("id" + i);
            }

            ExcerptTailer tailer = queue.createTailer();
            assertNext(tailer, "id1");
            long index = tailer.index();
            assertNext(tailer, "id2");
            tailer.moveToIndex(index);
            assertNext(tailer, "id2");
            tailer.moveToIndex(index);
            assertNext(tailer, "id2");
        }
    }

    private void assertNext(ExcerptTailer tailer, String expected) {
        String next = tailer.readText();
        assertEquals(expected, next);
    }
}
