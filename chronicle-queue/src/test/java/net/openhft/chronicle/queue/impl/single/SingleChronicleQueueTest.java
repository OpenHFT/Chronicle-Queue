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
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class SingleChronicleQueueTest extends ChronicleQueueTestBase {

    enum TestKey implements WireKey {
        test
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
            { WireType.TEXT   },
            { WireType.BINARY }
        });
    }

    // *************************************************************************
    //
    // *************************************************************************

    private final WireType wireType;

    /**
     *
     * @param wireType
     */
    public SingleChronicleQueueTest(WireType wireType) {
        this.wireType = wireType;
    }

    // *************************************************************************
    //
    // *************************************************************************

    @Test
    public void testAppend() throws IOException {
        final ChronicleQueue queue = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .build();

        final ExcerptAppender appender = queue.createAppender();
        for(int i=0; i<10; i++) {
            final int n = i;
            assertEquals(n, appender.writeDocument(w -> w.write(TestKey.test).int32(n)));
        }
    }

    @Test
    public void testAppendAndRead() throws IOException {
        final ChronicleQueue queue = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .build();

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
        
        final ChronicleQueue queue = new SingleChronicleQueueBuilder(getTmpDir())
            .wireType(this.wireType)
            .rollCycle(RollCycles.SECONDS)
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
    public void testAppendAndReadWithRolling2() throws IOException {
        final File dir = getTmpDir();

        for(int i=0; i<10; i++) {
            final int n = i;

            new SingleChronicleQueueBuilder(dir)
                .wireType(this.wireType)
                .rollCycle(RollCycles.SECONDS)
                .build()
                .createAppender().writeDocument(w -> w.write(TestKey.test).int32(n));

            Jvm.pause(500);
        }

        final ChronicleQueue queue = new SingleChronicleQueueBuilder(dir)
            .wireType(this.wireType)
            .rollCycle(RollCycles.SECONDS)
            .build();

        final ExcerptTailer tailer =queue.createTailer().toStart();
        for(int i=0; i<10; i++) {
            final int n = i;
            assertTrue(tailer.readDocument(r -> assertEquals(n, r.read(TestKey.test).int32())));
        }
    }

    @Test
    public void testAppendAndReadAtIndex() throws IOException {
        final ChronicleQueue queue = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .build();

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
