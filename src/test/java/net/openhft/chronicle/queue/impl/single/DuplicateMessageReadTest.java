/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
        expectException("This functionality has been deprecated and in future will only be available in Chronicle Queue Enterprise");
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