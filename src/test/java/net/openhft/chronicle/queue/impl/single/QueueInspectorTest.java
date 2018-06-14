package net.openhft.chronicle.queue.impl.single;

import net.openhft.affinity.Affinity;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.ValueOut;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

public final class QueueInspectorTest {
    private static final String PROPERTY_KEY = "wire.encodeTidInHeader";
    private static String previousValue = null;

    @BeforeClass
    public static void enableFeature() {
        previousValue = System.getProperty(PROPERTY_KEY);
        System.setProperty(PROPERTY_KEY, Boolean.TRUE.toString());
    }

    @AfterClass
    public static void resetFeature() {
        if (previousValue != null) {
            System.setProperty(PROPERTY_KEY, previousValue);
        } else {
            System.clearProperty(PROPERTY_KEY);
        }
    }

    @Test
    public void shouldDetermineWritingProcessIdWhenDocumentIsNotComplete() throws IOException {

        try (final SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(getTmpDir()).
                testBlockSize().
                build()) {
            final QueueInspector inspector = new QueueInspector(queue);
            final ExcerptAppender appender = queue.acquireAppender();
            appender.writeDocument(37L, ValueOut::int64);
            try (final DocumentContext ctx = appender.writingDocument()) {
                ctx.wire().write("foo").int32(17L);
                final int writingThreadId = inspector.getWritingThreadId();
                assertThat(writingThreadId, is(Affinity.getThreadId()));
                assertThat(QueueInspector.isValidThreadId(writingThreadId), is(true));
            }
        }
    }

    @Test
    public void shouldIndicateNoProcessIdWhenDocumentIsComplete() throws IOException {
        try (final SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(getTmpDir()).
                testBlockSize().
                build()) {
            final QueueInspector inspector = new QueueInspector(queue);
            final ExcerptAppender appender = queue.acquireAppender();
            appender.writeDocument(37L, ValueOut::int64);
            try (final DocumentContext ctx = appender.writingDocument()) {
                ctx.wire().write("foo").int32(17L);
            }
            final int writingThreadId = inspector.getWritingThreadId();
            assertThat(writingThreadId, is(not(OS.getProcessId())));
            assertThat(QueueInspector.isValidThreadId(writingThreadId), is(false));
        }
    }

    protected File getTmpDir() {
        final String methodName = "QueueInspectorTest";
        return DirectoryUtils.tempDir(methodName != null ?
                methodName.replaceAll("[\\[\\]\\s]+", "_") : "NULL-" + UUID.randomUUID());
    }

}