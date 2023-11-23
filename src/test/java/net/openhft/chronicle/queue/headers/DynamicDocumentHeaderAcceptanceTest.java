package net.openhft.chronicle.queue.headers;

import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

/**
 * Basic acceptance tests that check whether the frame has been corrupted by adding the checksum.
 */
public class DynamicDocumentHeaderAcceptanceTest extends QueueTestCommon {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public TestName testName = new TestName();

    private SingleChronicleQueue queue;

    private ExcerptAppender appender;

    private ExcerptTailer tailer;

    @Before
    public void before() {
        queue = SingleChronicleQueueBuilder.builder().path(Paths.get(temporaryFolder.getRoot().toString(), testName.getMethodName())).build();
        appender = queue.createAppender();
        tailer = queue.createTailer();
    }

    @After
    public void after() {
        appender.close();
        tailer.close();
        queue.close();
    }

    @Test
    public void textCase() {
        appender.writeText("Hello");
        assertEquals("Hello", tailer.readText());
    }

    @Test
    public void textCase_multipleEntries() {
        appender.writeText("1");
        appender.writeText("2");
        appender.writeText("3");
        assertEquals("1", tailer.readText());
        assertEquals("2", tailer.readText());
        assertEquals("3", tailer.readText());
    }

    @Test
    public void insertMetadataInStream() {
        appender.writeText("1");
        try (DocumentContext context = appender.writingDocument(true)) {
            context.wire().bytes().write("Test");
        }
        appender.writeText("2");
        assertEquals("1", tailer.readText());
        assertEquals("2", tailer.readText());
    }

}
