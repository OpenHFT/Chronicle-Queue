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
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class AsyncChronicleQueueTest extends ChronicleQueueTestBase {

    @Ignore
    @Test
    public void testAppendAndRead() throws IOException {
        final ChronicleQueue queue = SingleChronicleQueueBuilder.text(getTmpDir()).build();
        final ChronicleQueue async = new AsyncChronicleQueueBuilder(queue).build();

        final ExcerptAppender appender = async.createAppender();
        for (int i = 0; i < 10; i++) {
            final int n = i;
            appender.writeDocument(w -> w.write(TestKey.test).int32(n));
        }

        final ExcerptTailer tailer = queue.createTailer();
        for (int i = 0; i < 10;) {
            final int n = i;
            if(tailer.readDocument(r -> assertEquals(n, r.read(TestKey.test).int32()))) {
                i++;
            }
        }
    }


    @Ignore
    @Test
    public void testAppendAndReadX() throws IOException {
        final ChronicleQueue queue = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(WireType.TEXT)
                .blockSize(640_000)
                .build();

        final ExcerptAppender appender = queue.createAppender();
        for (int i = 0; i < 10; i++) {
            final int n = i;
            assertEquals(n, appender.writeDocument(w -> w.write(TestKey.test).int32(n)));
            assertEquals(n, appender.index());
        }

        final ExcerptTailer tailer = queue.createTailer();

        // Sequential read
        for (int i = 0; i < 10; i++) {
            final int n = i;
            assertTrue(tailer.readDocument(r -> assertEquals(n, r.read(TestKey.test).int32())));
            assertEquals(n, tailer.index());
        }

        // Random read
        for (int i = 0; i < 10; i++) {
            final int n = i;
            assertTrue(tailer.index(n));
            assertTrue(tailer.readDocument(r -> assertEquals(n, r.read(TestKey.test).int32())));
            assertEquals(n, tailer.index());
        }
    }
}
