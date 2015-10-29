/*
 *
 *    Copyright (C) 2015  higherfrequencytrading.com
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU Lesser General Public License as published by
 *    the Free Software Foundation, either version 3 of the License.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Lesser General Public License for more details.
 *
 *    You should have received a copy of the GNU Lesser General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueTestBase;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.async.AsyncChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class AsyncChronicleQueueTest extends ChronicleQueueTestBase {

    @Test
    public void testAppendAndRead() throws IOException {
        final ChronicleQueue queue = SingleChronicleQueueBuilder.text(getTmpDir()).build();
        final ChronicleQueue async = new AsyncChronicleQueueBuilder(queue).build();
        final int iterations = 500;

        final ExcerptAppender appender = async.createAppender();
        for (int i = 0; i < iterations; i++) {
            final int n = i;
            appender.writeDocument(w -> w.write(TestKey.test).int32(n));
        }

        final ExcerptTailer tailer = queue.createTailer();
        for (int i = 0; i < iterations;) {
            final int n = i;
            if(tailer.readDocument(r -> assertEquals(n, r.read(TestKey.test).int32()))) {
                i++;
            }
        }
    }



    @Test
    public void testAppendAndReadWithBytes() throws IOException {
        final ChronicleQueue queue = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(WireType.TEXT)
                .build();

        final ExcerptAppender appender = queue.createAppender();
        for (int i = 0; i < 10; i++) {
            final int n = i;
            assertEquals(n, appender.writeBytes(wb -> wb.writeInt(n)));
            assertEquals(n, appender.index());
        }

        final ExcerptTailer tailer = queue.createTailer();

        // Sequential read
        for (int i = 0; i < 10; i++) {
            final int n = i;
            assertTrue(tailer.readBytes(b -> assertEquals(n, b.readInt())));
            assertEquals(n, tailer.index());
        }

        // Random read
        for (int i = 0; i < 10; i++) {
            final int n = i;
            assertTrue(tailer.index(n));
            assertTrue(tailer.readBytes(b -> assertEquals(n, b.readInt())));
            assertEquals(n, tailer.index());
        }
    }
}
