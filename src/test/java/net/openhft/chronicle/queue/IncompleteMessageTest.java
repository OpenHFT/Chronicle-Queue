package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.ValueOut;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class IncompleteMessageTest extends QueueTestCommon {
    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    @Test
    public void incompleteMessageShouldBeSkipped() throws Exception {
        expectException("Couldn't acquire write lock after ");
        expectException("Forced unlock for the lock ");
        ignoreException("Unable to release the lock");
        try (SingleChronicleQueue queue = createQueue()) {
            try (ExcerptAppender appender = queue.acquireAppender()) {
                appender.writeDocument("hello", ValueOut::text);

                // open a document context, but do not close
                final DocumentContext documentContext = appender.writingDocument();
                documentContext.wire().bytes().write("incomplete longer write".getBytes(StandardCharsets.UTF_8));
            }
        }

        try (SingleChronicleQueue queue = createQueue()) {
            try (ExcerptAppender appender = queue.acquireAppender()) {
                appender.writeDocument("world", ValueOut::text);
            }

            try (ExcerptTailer tailer = queue.createTailer()) {
                tailer.toStart();
                assertEquals("hello", tailer.readText());
                assertEquals("world", tailer.readText());
                assertFalse(tailer.readingDocument().isPresent());
            }
        }
    }

    private SingleChronicleQueue createQueue() {
        return SingleChronicleQueueBuilder.binary(tmpDir.getRoot()).timeoutMS(250).build();
    }
}
