/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.wire.WireKey;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SingleChronicleQueueTest extends ChronicleQueueTestBase {

    enum TestKey implements WireKey {
        test
    }

    // *************************************************************************
    //
    // *************************************************************************

    @Test
    public void testAppend() throws IOException {
        final ChronicleQueue queue = SingleChronicleQueueBuilder.text(getTmpDir()).build();

        final ExcerptAppender appender = queue.createAppender();
        for(int i=0; i<10; i++) {
            final int n = i;
            assertEquals(n, appender.writeDocument(w -> w.write(TestKey.test).int32(n)));
        }
    }

    @Test
    public void testAppendAndRead() throws IOException {
        final ChronicleQueue queue = SingleChronicleQueueBuilder.text(getTmpDir()).build();

        final ExcerptAppender appender = queue.createAppender();
        for(int i=0; i<2; i++) {
            final int n = i;
            assertEquals(n, appender.writeDocument(w -> w.write(TestKey.test).int32(n)));
            assertEquals(n, appender.lastWrittenIndex());
        }

        final ExcerptTailer tailer =queue.createTailer();
        for(int i=0; i<2; i++) {
            final int n = i;
            assertTrue(tailer.readDocument(r -> assertEquals(n, r.read(TestKey.test).int32())));
        }
    }

    @Test
    public void testAppendAndReadWithRolling() throws IOException {
        final ChronicleQueue queue = SingleChronicleQueueBuilder.text(getTmpDir())
            .rollCycle(RollCycle.SECONDS)
            .build();

        final ExcerptAppender appender = queue.createAppender();
        for(int i=0; i<20; i++) {
            final int n = i;
            Jvm.pause(500);
            appender.writeDocument(w -> w.write(TestKey.test).int32(n));
        }

        final ExcerptTailer tailer =queue.createTailer().toStart();
        for(int i=0; i<20; i++) {
            final int n = i;
            assertTrue(tailer.readDocument(r -> assertEquals(n, r.read(TestKey.test).int32())));
        }
    }

    @Test
    public void testAppendAndReadAtIndex() throws IOException {
        final ChronicleQueue queue = SingleChronicleQueueBuilder.text(getTmpDir()).build();

        final ExcerptAppender appender = queue.createAppender();
        for(int i=0; i<5; i++) {
            final int n = i;
            assertEquals(n, appender.writeDocument(w -> w.write(TestKey.test).int32(n)));
            assertEquals(n, appender.lastWrittenIndex());
        }

        final ExcerptTailer tailer = queue.createTailer();
        for(int i=0; i<5; i++) {
            assertTrue(tailer.index(i));

            final int n = i;
            assertTrue(tailer.readDocument(r -> assertEquals(n, r.read(TestKey.test).int32())));
        }
    }
}
