package net.openhft.chronicle.queue;

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
public class ChecksumSuffixTest extends QueueTestCommon {

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
    public void intCase() {
        try (DocumentContext context = appender.writingDocument()) {
            context.wire().bytes().writeInt(42);
        }

        tailer.readBytes(bytes -> {
            assertEquals(42, bytes.readInt());
        });
    }

    @Test
    public void textCase() {
        appender.writeText("Hello");
        assertEquals("Hello", tailer.readText());
    }

}
