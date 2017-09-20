package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@Ignore
public final class DuplicateMessageReadTest {

    @Test
    public void shouldNotReceiveDuplicateMessages() throws Exception {
        final File location = DirectoryUtils.tempDir(DuplicateMessageReadTest.class.getSimpleName());

        final SingleChronicleQueue chronicleQueue = SingleChronicleQueueBuilder
                .binary(location)
                .rollCycle(RollCycles.HOURLY)
                .build();

        // this warmup with a different appender creates the problem
        final ExcerptAppender preTouchAppender = chronicleQueue.acquireAppender();
        preTouchAppender.pretouch();


        System.out.printf("First appender: %s%n", preTouchAppender);

        final List<Data> expected = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            expected.add(new Data(i));
        }

        final ExcerptTailer tailer = chronicleQueue.createTailer();
        tailer.toEnd(); // move to end of chronicle before writing

        // use different appender to write data
        final ExcerptAppender appender = chronicleQueue.acquireAppender();
        System.out.printf("Writing appender: %s%n", appender);
        for (final Data data : expected) {
            write(appender, data);
        }

        final List<Data> actual = new ArrayList<>();
        for (Data data = read(tailer); data != null; data = read(tailer)) {
            actual.add(data);
        }

        assertThat(actual, is(expected));
    }

    private static void write(final ExcerptAppender appender, final Data data) throws Exception {
        try (final DocumentContext dc = appender.writingDocument()) {
            final ObjectOutput out = dc.wire().objectOutput();
            out.writeInt(data.id);
        }
    }

    private static Data read(final ExcerptTailer tailer) throws Exception {
        try (final DocumentContext dc = tailer.readingDocument()) {
            if (!dc.isPresent()) {
                return null;
            }
            final ObjectInput in = dc.wire().objectInput();
            return new Data(in.readInt());
        }
    }

    private static final class Data {
        private final int id;

        private Data(final int id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Data data = (Data) o;

            return id == data.id;
        }

        @Override
        public int hashCode() {
            return id;
        }

        @Override
        public String toString() {
            return "" + id;
        }
    }
}