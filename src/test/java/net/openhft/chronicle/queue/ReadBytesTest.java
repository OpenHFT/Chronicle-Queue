package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.*;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.InvalidMarshallableException;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

/**
 * <h1>Background</h1>
 * There are two readBytes methods with ExcerptTailer. These have slightly different semantics. This test exists to
 * demonstrate the differences.
 */
public class ReadBytesTest extends QueueTestCommon {

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
    public void writeDocument_readBytesMarshallable() {
        try (DocumentContext context = appender.writingDocument()) {
            context.wire().bytes().writeInt(42);
        }

        tailer.readBytes(bytes -> assertEquals(42, bytes.readInt()));
    }

    @Test
    public void writeDocument_readBytes_headerIsNotInOutputBytes() {
        try (DocumentContext context = appender.writingDocument()) {
            context.wire().bytes().writeInt(42);
        }

        Bytes<?> in = Bytes.elasticByteBuffer();
        tailer.readBytes(in);
        assertEquals(42, in.readInt());
        in.releaseLast();
    }

    @Test
    public void writeBytes_readBytes_headerIsInOutputBytes() {
        try (DocumentContext context = appender.writingDocument()) {
            context.wire().writeBytes(bytes -> bytes.writeInt(42));
            context.wire().writeText("hello");
        }

        Bytes<?> in = Bytes.elasticByteBuffer();
        tailer.readBytes(in);
        assertEquals(4, in.readInt()); // This is the length of the byte stream written by writeBytes
        assertEquals(42, in.readInt());
        in.releaseLast();
    }

}
