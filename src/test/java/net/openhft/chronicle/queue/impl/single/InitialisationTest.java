package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import org.junit.Ignore;
import org.junit.Test;

import static net.openhft.chronicle.queue.DirectoryUtils.tempDir;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public final class InitialisationTest {
    @Ignore
    @Test
    public void shouldRecoverIfSpecifiedWireTypeDoesNotMatchPersistedFileWireType() throws Exception {
        final SingleChronicleQueueBuilder builder = builder(WireType.BINARY);

        try (final SingleChronicleQueue textQueue = builder.build()) {

            try (final SingleChronicleQueue binaryQueue =
                         SingleChronicleQueueBuilder.builder(textQueue.path, WireType.TEXT).build()) {
                try (final DocumentContext ctx = binaryQueue.acquireAppender().writingDocument()) {
                    ctx.wire().write("key").text("value");
                }
            }

            final ExcerptTailer tailer = textQueue.createTailer();
            try (final DocumentContext ctx = tailer.readingDocument()) {
                assertTrue(ctx.isPresent());

                assertThat(ctx.wire().read("key"), is("value"));
            }
        }
    }

    private SingleChronicleQueueBuilder builder(final WireType wireType) {
        return SingleChronicleQueueBuilder.
                builder(tempDir(InitialisationTest.class.getSimpleName() + System.nanoTime()), wireType);
    }
}
