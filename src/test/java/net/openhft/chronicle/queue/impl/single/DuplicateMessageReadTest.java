package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public final class DuplicateMessageReadTest extends ChronicleQueueTestBase {
    private static final RollCycles QUEUE_CYCLE = RollCycles.DAILY;

    private static void write(final ExcerptAppender appender, final Data data) throws IOException {
        try (final DocumentContext dc = appender.writingDocument()) {
            final ObjectOutput out = dc.wire().objectOutput();
            out.writeInt(data.id);
        }
    }

    private static Data read(final ExcerptTailer tailer) throws IOException {
        try (final DocumentContext dc = tailer.readingDocument()) {
            if (!dc.isPresent()) {
                return null;
            }

            final ObjectInput in = dc.wire().objectInput();
            return new Data(in.readInt());
        }
    }

    @Test
    public void shouldNotReceiveDuplicateMessages() throws IOException {
        final File location = getTmpDir();

        try (final ChronicleQueue chronicleQueue = SingleChronicleQueueBuilder
                .binary(location)
                .rollCycle(QUEUE_CYCLE)
                .build()) {

            final ExcerptAppender appender = chronicleQueue.acquireAppender();
            appender.pretouch();

            final List<Data> expected = new ArrayList<>();
            for (int i = 50; i < 60; i++) {
                expected.add(new Data(i));
            }

            final ExcerptTailer tailer = chronicleQueue.createTailer();
            tailer.toEnd(); // move to end of chronicle before writing

            for (final Data data : expected) {
                write(appender, data);
            }

            final List<Data> actual = new ArrayList<>();
            Data data;
            while ((data = read(tailer)) != null) {
                actual.add(data);
            }

            assertEquals(expected, actual);
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