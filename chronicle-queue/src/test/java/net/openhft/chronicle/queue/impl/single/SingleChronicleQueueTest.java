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
import net.openhft.chronicle.queue.impl.single.work.in.progress.IndexedSingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.work.in.progress.Indexer;
import net.openhft.chronicle.wire.WireKey;
import net.openhft.chronicle.wire.WireType;
import org.junit.Assert;
import org.junit.Ignore;
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
                {WireType.TEXT},
                {WireType.BINARY}
        });
    }

    // *************************************************************************
    //
    // *************************************************************************

    private final WireType wireType;

    /**
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
        for (int i = 0; i < 10; i++) {
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
        for (int i = 0; i < 2; i++) {
            final int n = i;
            assertEquals(n, appender.writeDocument(w -> w.write(TestKey.test).int32(n)));
            assertEquals(n, appender.lastWrittenIndex());
        }

        final ExcerptTailer tailer = queue.createTailer();
        for (int i = 0; i < 2; i++) {
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
        for (int i = 0; i < 20; i++) {
            final int n = i;
            Jvm.pause(500);
            appender.writeDocument(w -> w.write(TestKey.test).int32(n));
        }

        final ExcerptTailer tailer = queue.createTailer().toStart();
        for (int i = 0; i < 20; i++) {
            final int n = i;
            assertTrue(tailer.readDocument(r -> assertEquals(n, r.read(TestKey.test).int32())));
        }
    }

    @Test
    public void testAppendAndReadWithRolling2() throws IOException {
        final File dir = getTmpDir();

        for (int i = 0; i < 10; i++) {
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

        final ExcerptTailer tailer = queue.createTailer().toStart();
        for (int i = 0; i < 10; i++) {
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
        for (int i = 0; i < 5; i++) {
            final int n = i;
            assertEquals(n, appender.writeDocument(w -> w.write(TestKey.test).int32(n)));
            assertEquals(n, appender.lastWrittenIndex());
        }

        final ExcerptTailer tailer = queue.createTailer();
        for (int i = 0; i < 5; i++) {
            assertTrue(tailer.index(i));

            final int n = i;
            assertTrue(tailer.readDocument(r -> assertEquals(n, r.read(TestKey.test).int32())));
        }
    }

    @Test
    public void testSimpleWire() throws Exception {

        File file = File.createTempFile("chronicle.", "q");
        file.deleteOnExit();

        try {

            final ChronicleQueue chronicle = new SingleChronicleQueueBuilder(getTmpDir())
                    .wireType(this.wireType)
                    .build();

            final ExcerptAppender appender = chronicle.createAppender();
            appender.writeDocument(wire -> wire.write(() -> "FirstName").text("Steve"));
            appender.writeDocument(wire -> wire.write(() -> "Surname").text("Jobs"));

            StringBuilder first = new StringBuilder();
            StringBuilder surname = new StringBuilder();

            final ExcerptTailer tailer = chronicle.createTailer();

            tailer.readDocument(wire -> wire.read(() -> "FirstName").text(first));
            tailer.readDocument(wire -> wire.read(() -> "Surname").text(surname));

            Assert.assertEquals("Steve Jobs", first + " " + surname);


        } finally {
            file.delete();
        }

    }


    @Test
    public void testReadAtIndex() throws Exception {

        final File file = File.createTempFile("chronicle.", "q");
        file.deleteOnExit();

        try {

            final ChronicleQueue chronicle = new SingleChronicleQueueBuilder(getTmpDir())
                    .wireType(this.wireType)
                    .build();
            final ExcerptAppender appender = chronicle.createAppender();

            // create 100 documents
            for (int i = 0; i < 100; i++) {
                final int j = i;
                appender.writeDocument(wire -> wire.write(() -> "key").text("value=" + j));
            }

            final ExcerptTailer tailer = chronicle.createTailer();
            tailer.index(5);

            //   QueueDumpMain.dump(file, new PrintWriter(System.out));

            StringBuilder sb = new StringBuilder();
            tailer.readDocument(wire -> wire.read(() -> "key").text(sb));

            Assert.assertEquals("value=5", sb.toString());


        } finally {
            file.delete();
        }

    }

    @Test
    public void testLastWrittenIndexPerAppender() throws Exception {

        File file = File.createTempFile("chronicle.", "q");
        file.deleteOnExit();
        try {

            final ChronicleQueue chronicle = new SingleChronicleQueueBuilder(getTmpDir())
                    .wireType(this.wireType)
                    .build();
            final ExcerptAppender appender = chronicle.createAppender();

            appender.writeDocument(wire -> wire.write(() -> "key").text("test"));
            Assert.assertEquals(0, appender.lastWrittenIndex());

        } finally {
            file.delete();
        }

    }

    @Test(expected = IllegalStateException.class)
    public void testLastWrittenIndexPerAppenderNoData() throws Exception {

        File file = File.createTempFile("chronicle.", "q");
        file.deleteOnExit();
        try {
            final ChronicleQueue chronicle = new SingleChronicleQueueBuilder(getTmpDir())
                    .wireType(this.wireType)
                    .build();
            final ExcerptAppender appender = chronicle.createAppender();
            appender.lastWrittenIndex();
            Assert.fail();
        } finally {
            file.delete();
        }

    }


    @Ignore
    @Test
    public void testLastIndexPerChronicle() throws Exception {

        File file = File.createTempFile("chronicle.", "q");
        file.deleteOnExit();
        try {

            final ChronicleQueue chronicle = new SingleChronicleQueueBuilder(getTmpDir())
                    .wireType(this.wireType)
                    .build();

            final ExcerptAppender appender = chronicle.createAppender();

            appender.writeDocument(wire -> wire.write(() -> "key").text("test"));
            Assert.assertEquals(0, chronicle.lastWrittenIndex());

        } finally {
            file.delete();
        }

    }

    @Ignore
    @Test(expected = IllegalStateException.class)
    public void testLastIndexPerChronicleNoData() throws Exception {

        File file = File.createTempFile("chronicle.", "q");
        file.deleteOnExit();
        try {

            final ChronicleQueue chronicle = new SingleChronicleQueueBuilder(getTmpDir())
                    .wireType(this.wireType)
                    .build();

            Assert.assertEquals(-1, chronicle.lastWrittenIndex());

        } finally {
            file.delete();
        }

    }


    @Test
    public void testHeaderIndexReadAtIndex() throws Exception {

        File file = File.createTempFile("chronicle.", "q");
        file.deleteOnExit();
        try {

            final ChronicleQueue chronicle = new SingleChronicleQueueBuilder(getTmpDir())
                    .wireType(this.wireType)
                    .build();

            final ExcerptAppender appender = chronicle.createAppender();

            // create 100 documents
            for (int i = 0; i < 100; i++) {
                final int j = i;
                appender.writeDocument(wire -> wire.write(() -> "key").text("value=" + j));
            }

            final ExcerptTailer tailer = chronicle.createTailer();
            tailer.index(5);

            StringBuilder sb = new StringBuilder();
            tailer.readDocument(wire -> wire.read(() -> "key").text(sb));

            Assert.assertEquals("value=5", sb.toString());

        } finally {
            file.delete();
        }

    }


    @Ignore
    @Test
    public void testReadAtIndexWithIndexes() throws Exception {

        File file = File.createTempFile("chronicle.", "q");
        file.deleteOnExit();
        try {

            final ChronicleQueue chronicle = new SingleChronicleQueueBuilder(getTmpDir())
                    .wireType(this.wireType)
                    .rollCycle(RollCycles.SECONDS)
                    .build();

            final ExcerptAppender appender = chronicle.createAppender();

            // create 100 documents
            for (int i = 0; i < 100; i++) {
                final int j = i;
                appender.writeDocument(wire -> wire.write(() -> "key").text("value=" + j));
            }


            // creates the indexes
            new Indexer(chronicle, WireType.BINARY).index();

            final ExcerptTailer tailer = chronicle.createTailer();

            tailer.index(67);

            StringBuilder sb = new StringBuilder();
            tailer.readDocument(wire -> wire.read(() -> "key").text(sb));

            Assert.assertEquals("value=67", sb.toString());

        } finally {
            file.delete();
        }

    }

    @Ignore
    @Test
    public void testReadAtIndexWithIndexesAtStart() throws Exception {


        final File file = File.createTempFile("chronicle.", "q");
        file.deleteOnExit();

        try {

            final ChronicleQueue chronicle = new SingleChronicleQueueBuilder(getTmpDir())
                    .wireType(this.wireType)
                    .rollCycle(RollCycles.SECONDS)
                    .build();

            final ExcerptAppender appender = chronicle.createAppender();

            // create 100 documents
            for (int i = 0; i < 100; i++) {
                final int j = i;
                appender.writeDocument(wire -> wire.write(() -> "key").text("value=" + j));
            }


            new Indexer(chronicle, WireType.BINARY).index();

            long index = 67;
            final ExcerptTailer tailer = chronicle.createTailer();
            tailer.index(index);

            //   QueueDumpMain.dump(file, new PrintWriter(System.out));

            StringBuilder sb = new StringBuilder();
            tailer.readDocument(wire -> wire.read(() -> "key").text(sb));

            Assert.assertEquals("value=" + index, sb.toString());


        } finally {
            file.delete();
        }


    }


    @Ignore
    @Test
    public void testScanFromLastKnownIndex() throws Exception {

        File file = File.createTempFile("chronicle.", "q");
        file.deleteOnExit();
        try {

            final ChronicleQueue chronicle = new IndexedSingleChronicleQueue(file.getPath(), 1024, WireType.BINARY);
            final ExcerptAppender appender = chronicle.createAppender();

            // create 100 documents
            for (int i = 0; i < 65; i++) {
                final int j = i;
                appender.writeDocument(wire -> wire.write(() -> "key").text("value=" + j));
            }

            // creates the indexes - index's 1 and 2 are created by the indexer
            new Indexer(chronicle, WireType.BINARY).index();

            // create 100 documents
            for (long i = chronicle.lastWrittenIndex() + 1; i < 200; i++) {
                final long j = i;
                appender.writeDocument(wire -> wire.write(() -> "key").text("value=" + j));
            }

            final ExcerptTailer tailer = chronicle.createTailer();
            {
                int expected = 150;
                tailer.index(expected);

                StringBuilder sb = new StringBuilder();
                tailer.readDocument(wire -> wire.read(() -> "key").text(sb));

                Assert.assertEquals("value=" + expected, sb.toString());
            }

            //read back earlier
            {
                int expected = 167;
                tailer.index(expected);

                StringBuilder sb = new StringBuilder();
                tailer.readDocument(wire -> wire.read(() -> "key").text(sb));

                Assert.assertEquals("value=" + expected, sb.toString());
            }

        } finally {
            file.delete();
        }

    }


    enum Field implements WireKey {
        TEXT
    }

}
