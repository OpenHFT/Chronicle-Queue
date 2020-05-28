package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.ValueIn;
import org.junit.Test;

import java.io.File;

import static net.openhft.chronicle.queue.RollCycles.TEST_DAILY;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public final class ExcerptsSkippedWhenTailerDirectionNoneTest {
    @Test
    public void shouldNotSkipMessageAtStartOfQueue() {
        final File tmpDir = DirectoryUtils.tempDir(ExcerptsSkippedWhenTailerDirectionNoneTest.class.getSimpleName());
        try (final ChronicleQueue writeQueue =
                     ChronicleQueue.singleBuilder(tmpDir)
                             .testBlockSize()
                             .rollCycle(TEST_DAILY)
                             .build()) {
            final ExcerptAppender excerptAppender = writeQueue.acquireAppender();
            try (final DocumentContext ctx = excerptAppender.writingDocument()) {
                ctx.wire().getValueOut().object("first");
            }
            try (final DocumentContext ctx = excerptAppender.writingDocument()) {
                ctx.wire().getValueOut().object("second");
            }
        }

        try (final ChronicleQueue readQueue =
                     ChronicleQueue.singleBuilder(tmpDir)
                             .testBlockSize()
                             .rollCycle(TEST_DAILY)
                             .build()) {

            final ExcerptTailer tailer = readQueue.createTailer();
            final RollCycle rollCycle = readQueue.rollCycle();
            assertThat(rollCycle.toSequenceNumber(tailer.index()), is(0L));
            try (final DocumentContext ctx = tailer.direction(TailerDirection.NONE).readingDocument()) {
                // access the first document without incrementing sequence number
            }
            assertThat(rollCycle.toSequenceNumber(tailer.index()), is(0L));

            String value;
            try (DocumentContext dc =
                         tailer.direction(TailerDirection.FORWARD).readingDocument()) {
                ValueIn valueIn = dc.wire().getValueIn();
                value = (String) valueIn.object();
            }
            assertThat(rollCycle.toSequenceNumber(tailer.index()), is(1L));

            assertThat(value, is("first"));

            try (DocumentContext dc =
                         tailer.direction(TailerDirection.NONE).readingDocument()) {
                ValueIn valueIn = dc.wire().getValueIn();
                value = (String) valueIn.object();
            }
            assertThat(rollCycle.toSequenceNumber(tailer.index()), is(1L));

            assertThat(value, is("second"));

            try (DocumentContext dc =
                         tailer.direction(TailerDirection.NONE).readingDocument()) {
                ValueIn valueIn = dc.wire().getValueIn();
                value = (String) valueIn.object();
            }
            assertThat(rollCycle.toSequenceNumber(tailer.index()), is(1L));

            assertThat(value, is("second"));
        }
    }
}
