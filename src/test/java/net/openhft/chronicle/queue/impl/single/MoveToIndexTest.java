package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

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

    // https://github.com/OpenHFT/Chronicle-Queue/issues/401
    @Ignore
    @Test
    public void testRandomMove() throws Exception {
        int nbMessages = 10;
        int nbTests = 100;

        Map<Long, String> messages = new HashMap<>(nbMessages);

        File cqFolder = tmpFolder.newFolder("cq");
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(cqFolder).build()) {
            // create a queue and add some excerpts
            ExcerptAppender app = queue.acquireAppender();
            for (int i =0; i < nbMessages; i++) {
                final String message = "msg" + i;
                app.writeDocument(w -> w.write("message").object(message));
                if (i < nbMessages + 1) {
                    messages.put(app.lastIndexAppended(), message);
                }
            }

            Random random = new Random(1510298038000L);
            List<Long> offsets = new ArrayList<>(messages.keySet());
            ExcerptTailer tailer = queue.createTailer().toStart();
            String[] ret = new String[1];
            for (int i=0; i < nbTests; i++) {
                long offset = offsets.get(random.nextInt(messages.keySet().size()));
                tailer.moveToIndex(offset);
                tailer.readDocument(w -> ret[0] = (String) w.read("message").object());
                assertEquals(messages.get(offset), ret[0]);
                long index = tailer.index();
                tailer.readDocument(w -> ret[0] = (String) w.read("message").object());
            }
        }
    }

    private void assertNext(ExcerptTailer tailer, String expected) {
        String next = tailer.readText();
        assertEquals(expected, next);
    }
}
