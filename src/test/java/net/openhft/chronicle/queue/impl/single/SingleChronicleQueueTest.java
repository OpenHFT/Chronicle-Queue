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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.queue.impl.RollingChronicleQueue.*;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class SingleChronicleQueueTest extends ChronicleQueueTestBase {

    private static final long TIMES = (4L << 20L);
    private final WireType wireType;

    /**
     * @param wireType the type of wire
     */
    public SingleChronicleQueueTest(@NotNull WireType wireType) {
        this.wireType = wireType;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {WireType.TEXT},
                {WireType.BINARY}
                //{ WireType.FIELDLESS_BINARY }
        });
    }

    // *************************************************************************
    //
    // TESTS
    //
    // *************************************************************************

    @Test
    public void testAppend() throws IOException {
        try (final ChronicleQueue queue = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .build()) {

            final ExcerptAppender appender = queue.createAppender();
            for (int i = 0; i < 10; i++) {
                final int n = i;
                assertEquals(n, toSequenceNumber(appender.writeDocument(w -> w
                        .write(TestKey.test).int32(n))));
            }
        }
    }


    @Test
    public void testReadingLessBytesThanWritten() throws IOException {
        try (final ChronicleQueue queue = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .build()) {

            final ExcerptAppender appender = queue.createAppender();

            final Bytes<byte[]> expected = Bytes.wrapForRead("some long message".getBytes());
            for (int i = 0; i < 10; i++) {

                appender.writeBytes(expected);
            }

            final ExcerptTailer tailer = queue.createTailer();

            // Sequential read
            for (int i = 0; i < 10; i++) {

                Bytes b = Bytes.allocateDirect(8);

                try {
                    tailer.readBytes(b);
                } catch (Error e) {

                }

                Assert.assertEquals(expected.readInt(0), b.readInt(0));
            }
        }
    }


    @Test
    public void testAppendAndRead() throws IOException {
        try (final ChronicleQueue queue = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .build()) {

            final ExcerptAppender appender = queue.createAppender();
            final long cycle = appender.cycle();
            for (int i = 0; i < 10; i++) {
                final int n = i;
                assertEquals(n, toSequenceNumber(appender.writeDocument(w
                        -> w.write(TestKey.test).int32(n))));
                assertEquals(n, toSequenceNumber(appender.index()));
            }

            final ExcerptTailer tailer = queue.createTailer();

            // Sequential read
            for (int i = 0; i < 10; i++) {
                final int n = i;
                assertTrue(tailer.readDocument(r -> assertEquals(n, r.read(TestKey.test).int32())));
                assertEquals(n, toSequenceNumber(tailer.index()));
            }

            // Random read
            for (int i = 0; i < 10; i++) {
                final int n = i;
                assertTrue(tailer.moveToIndex(index(cycle, n)));
                assertTrue(tailer.readDocument(r -> assertEquals(n, r.read(TestKey.test).int32())));
                assertEquals(n, toSequenceNumber(tailer.index()));
            }
        }
    }

    @Test
    public void testReadAndAppend() throws IOException {
        try (final ChronicleQueue queue = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .build()) {

            int[] results = new int[2];

            Thread t = new Thread(() -> {
                try {
                    final ExcerptTailer tailer = queue.createTailer();
                    for (int i = 0; i < 2; ) {
                        boolean read = tailer.readDocument(r -> {
                            int result = r.read(TestKey.test).int32();
                            results[result] = result;
                        });

                        if (read) {
                            i++;
                        } else {
                            // Pause for a little
                            Jvm.pause(10);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    assertTrue(false);
                }
            });
            t.setDaemon(true);
            t.start();

            //Give the tailer thread enough time to initialise before send
            //the messages
            Jvm.pause(1000);

            final ExcerptAppender appender = queue.createAppender();
            for (int i = 0; i < 2; i++) {
                final int n = i;
                appender.writeDocument(w -> w.write(TestKey.test).int32(n));
            }

            Jvm.pause(1000);

            assertArrayEquals(new int[]{0, 1}, results);
        }
    }


    @Test
    public void testAppendAndReadWithRolling() throws IOException, InterruptedException {

        try (final ChronicleQueue queue = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .rollCycle(RollCycles.SECONDS)
                .epoch(1452773025277L)
                .build()) {

            final ExcerptAppender appender = queue.createAppender();
            for (int i = 0; i < 5; i++) {
                final int n = i;
                Jvm.pause(500);
                appender.writeDocument(w -> w.write(TestKey.test).int32(n));
            }

            final ExcerptTailer tailer = queue.createTailer().toStart();
            for (int i = 0; i < 5; i++) {
                final int n = i;
                final boolean condition = tailer.readDocument(r -> assertEquals(n, r.read(TestKey.test).int32()));
                assertTrue(condition);
            }
        }
    }

    @Test
    public void testAppendAndReadWithRolling2() throws IOException, InterruptedException {
        final File dir = getTmpDir();

        try (final ChronicleQueue queue = new SingleChronicleQueueBuilder(dir)
                .wireType(this.wireType)
                .rollCycle(RollCycles.SECONDS)
                .epoch(1452701442361L)
                .build()) {

            final ExcerptAppender appender = queue.createAppender();
            for (int i = 0; i < 10; i++) {
                final int n = i;
                appender.writeDocument(w -> w.write(TestKey.test).int32(n));
                Jvm.pause(500);
            }


            final ExcerptTailer tailer = queue.createTailer().toStart();
            for (int i = 0; i < 10; i++) {
                final int n = i;
                try {
                    final boolean condition = tailer.readDocument(
                            r -> assertEquals(n, r.read(TestKey.test).int32()));
                    assertTrue(condition);
                } catch (IllegalStateException e) {
                    e.printStackTrace();
                }

            }
        }
    }


    @Test
    public void testAppendAndReadAtIndex() throws IOException {
        try (final ChronicleQueue queue = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .build()) {

            final ExcerptAppender appender = queue.createAppender();
            appender.cycle();
            for (int i = 0; i < 5; i++) {
                final int n = i;
                assertEquals(n, toSequenceNumber(appender.writeDocument(w -> w.write(TestKey.test)
                        .int32(n))));
                assertEquals(n, toSequenceNumber(appender.index()));
            }

            final ExcerptTailer tailer = queue.createTailer();
            for (int i = 0; i < 5; i++) {
                final long index = index(appender.cycle(), i);
                assertTrue(tailer.moveToIndex(index));

                final int n = i;
                assertTrue(tailer.readDocument(r -> assertEquals(n, toSequenceNumber(r.read(TestKey.test)
                        .int32()))));
                assertEquals(n, toSequenceNumber(tailer.index()));
            }
        }
    }

    @Test
    public void testSimpleWire() throws IOException {
        try (final ChronicleQueue chronicle = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .build()) {

            final ExcerptAppender appender = chronicle.createAppender();
            appender.writeDocument(wire -> wire.write(() -> "FirstName").text("Steve"));
            appender.writeDocument(wire -> wire.write(() -> "Surname").text("Jobs"));

            StringBuilder first = new StringBuilder();
            StringBuilder surname = new StringBuilder();

            final ExcerptTailer tailer = chronicle.createTailer();

            tailer.readDocument(wire -> wire.read(() -> "FirstName").text(first));
            tailer.readDocument(wire -> wire.read(() -> "Surname").text(surname));
            Assert.assertEquals("Steve Jobs", first + " " + surname);
        }
    }


    @Test
    public void testSimpleByteTest() throws IOException {
        try (final ChronicleQueue chronicle = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .build()) {

            final ExcerptAppender appender = chronicle.createAppender();
            appender.writeBytes(Bytes.allocateDirect("Steve".getBytes()));
            appender.writeBytes(Bytes.allocateDirect("Jobs".getBytes()));
            final ExcerptTailer tailer = chronicle.createTailer();
            Bytes bytes = Bytes.elasticByteBuffer();
            tailer.readBytes(bytes);
            bytes.append(" ");
            tailer.readBytes(bytes);

            Assert.assertEquals("Steve Jobs", bytes.toString());
        }
    }


    @Test
    public void testReadAtIndex() throws IOException {
        try (final ChronicleQueue chronicle = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .build()) {
            final ExcerptAppender appender = chronicle.createAppender();

            long lastIndex = -1;
            // create 100 documents
            for (int i = 0; i < 100; i++) {
                final int j = i;
                lastIndex = appender.writeDocument(wire -> wire.write(() -> "key").text("value=" + j));
            }

            final long cycle = toCycle(lastIndex);
            final ExcerptTailer tailer = chronicle.createTailer();

            //   QueueDumpMain.dump(file, new PrintWriter(System.out));

            StringBuilder sb = new StringBuilder();

            for (int i : new int[]{0, 64, 65, 66}) {
                tailer.moveToIndex(index(cycle, i));
                tailer.readDocument(wire -> wire.read(() -> "key").text(sb));
                Assert.assertEquals("value=" + i, sb.toString());
            }
        }
    }

    @Ignore("long running test")
    @Test
    public void testReadAtIndex4MB() throws IOException {
        try (final ChronicleQueue chronicle = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .build()) {
            final ExcerptAppender appender = chronicle.createAppender();

            long lastIndex = -1;
            System.out.print("Percent written=");

            // create 100 documents
            for (long i = 0; i < TIMES; i++) {
                final long j = i;
                lastIndex = appender.writeDocument(wire -> wire.write(() -> "key").text("value=" + j));

                if (i % (TIMES / 20) == 0) {
                    System.out.println("" + (i * 100 / TIMES) + "%, ");
                }
            }


            final long cycle = toCycle(lastIndex);

            final ExcerptTailer tailer = chronicle.createTailer();

            //   QueueDumpMain.dump(file, new PrintWriter(System.out));

            StringBuilder sb = new StringBuilder();

            for (long i = 0; i < (4L << 20L); i++) {
                tailer.moveToIndex(index(cycle, i));
                tailer.readDocument(wire -> wire.read(() -> "key").text(sb));
                Assert.assertEquals("value=" + i, sb.toString());
                if (i % (TIMES / 20) == 0) {
                    System.out.println("Percent read= " + (i * 100 / TIMES) + "%");
                }
            }
        }
    }

    @Test
    public void testLastWrittenIndexPerAppender() throws IOException {
        try (final ChronicleQueue chronicle = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .build()) {
            final ExcerptAppender appender = chronicle.createAppender();

            appender.writeDocument(wire -> wire.write(() -> "key").text("test"));
            Assert.assertEquals(0, toSequenceNumber(appender.index()));
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testLastWrittenIndexPerAppenderNoData() throws IOException {
        try (final ChronicleQueue chronicle = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .build()) {
            final ExcerptAppender appender = chronicle.createAppender();
            appender.index();
            Assert.fail();
        }
    }

    @Test
    public void testLastIndexPerChronicle() throws IOException {
        try (final ChronicleQueue chronicle = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .build()) {

            final ExcerptAppender appender = chronicle.createAppender();

            final long index = appender.writeDocument(wire -> wire.write(() -> "key").text("test"));
            Assert.assertEquals(index, chronicle.lastIndex());
        }
    }

    @Test
    public void testHeaderIndexReadAtIndex() throws IOException {
        try (final ChronicleQueue chronicle = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .build()) {

            final ExcerptAppender appender = chronicle.createAppender();
            final long cycle = appender.cycle();
            // create 100 documents
            for (int i = 0; i < 100; i++) {
                final int j = i;
                appender.writeDocument(wire -> wire.write(() -> "key").text("value=" + j));
            }

            final ExcerptTailer tailer = chronicle.createTailer();
            tailer.moveToIndex(index(cycle, 0));

            StringBuilder sb = new StringBuilder();
            tailer.readDocument(wire -> wire.read(() -> "key").text(sb));

            Assert.assertEquals("value=0", sb.toString());
        }
    }


    /**
     * test that if we make EPOC the current time, then the cycle is == 0
     *
     * @throws IOException
     */
    @Test
    public void testEPOC() throws IOException {
        try (final ChronicleQueue chronicle = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .epoch(System.currentTimeMillis())
                .rollCycle(RollCycles.HOURS)
                .build()) {

            final ExcerptAppender appender = chronicle.createAppender();
            appender.writeDocument(wire -> wire.write(() -> "key").text("value=v"));
            Assert.assertTrue(appender.cycle() == 0);
        }

    }

    @Test
    public void testIndex() throws IOException {
        try (final ChronicleQueue chronicle = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .rollCycle(RollCycles.HOURS)
                .build()) {

            final ExcerptAppender appender = chronicle.createAppender();
            long cycle = appender.cycle();

            // create 100 documents
            for (int i = 0; i < 5; i++) {
                final int j = i;
                long index = appender.writeDocument(wire -> wire.write(() -> "key").text("value=" + j));
                if (i == 2) {
                    final long cycle1 = toCycle(index);
                    Assert.assertEquals(cycle1, cycle);
                }
            }

            final ExcerptTailer tailer = chronicle.createTailer();
            tailer.moveToIndex(index(cycle, 2));

            StringBuilder sb = new StringBuilder();
            tailer.readDocument(wire -> wire.read(() -> "key").text(sb));
            Assert.assertEquals("value=2", sb.toString());

            tailer.readDocument(wire -> wire.read(() -> "key").text(sb));
            Assert.assertEquals("value=3", sb.toString());

            tailer.readDocument(wire -> wire.read(() -> "key").text(sb));
            Assert.assertEquals("value=4", sb.toString());
        }
    }


    @Test
    public void testReadingDocument() throws IOException {
        try (final ChronicleQueue chronicle = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .rollCycle(RollCycles.HOURS)
                .build()) {

            final ExcerptAppender appender = chronicle.createAppender();
            long cycle = appender.cycle();

            // create 100 documents
            for (int i = 0; i < 5; i++) {
                final int j = i;
                long index = appender.writeDocument(wire -> wire.write(() -> "key").text("value=" + j));
                if (i == 2) {
                    final long cycle1 = toCycle(index);
                    Assert.assertEquals(cycle1, cycle);
                }
            }

            final ExcerptTailer tailer = chronicle.createTailer();


            final StringBuilder sb = Wires.acquireStringBuilder();

            try (final DocumentContext dc = tailer.readingDocument()) {
                assert dc.isPresent();
                assert dc.isData();
                dc.wire().read(() -> "key").text(sb);
                Assert.assertEquals("value=0", sb.toString());
            }

            try (final DocumentContext dc = tailer.readingDocument()) {
                assert dc.isPresent();
                assert dc.isData();
                dc.wire().read(() -> "key").text(sb);
                Assert.assertEquals("value=1", sb.toString());
            }

            try (final DocumentContext dc = tailer.readingDocument()) {
                assert dc.isPresent();
                assert dc.isData();
                dc.wire().read(() -> "key").text(sb);
                Assert.assertEquals("value=2", sb.toString());
            }

            try (final DocumentContext dc = tailer.readingDocument()) {
                assert dc.isPresent();
                assert dc.isData();
                dc.wire().read(() -> "key").text(sb);
                Assert.assertEquals("value=3", sb.toString());
            }

            try (final DocumentContext dc = tailer.readingDocument()) {
                assert dc.isPresent();
                assert dc.isData();
                dc.wire().read(() -> "key").text(sb);
                Assert.assertEquals("value=4", sb.toString());
            }

            try (final DocumentContext dc = tailer.readingDocument()) {
                assert !dc.isPresent();
                assert !dc.isData();
                assert !dc.isMetaData();
            }
        }
    }


    @Test
    public void testReadingDocumentWithFirstAMove() throws IOException {

        try (final ChronicleQueue chronicle = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .rollCycle(RollCycles.HOURS)
                .build()) {

            final ExcerptAppender appender = chronicle.createAppender();
            long cycle = appender.cycle();

            // create 100 documents
            for (int i = 0; i < 5; i++) {
                final int j = i;
                long index = appender.writeDocument(wire -> wire.write(() -> "key").text("value=" + j));
                if (i == 2) {
                    final long cycle1 = toCycle(index);
                    Assert.assertEquals(cycle1, cycle);
                }
            }

            final ExcerptTailer tailer = chronicle.createTailer();
            tailer.moveToIndex(index(cycle, 2));

            final StringBuilder sb = Wires.acquireStringBuilder();

            try (final DocumentContext dc = tailer.readingDocument()) {
                assert dc.isPresent();
                assert dc.isData();
                dc.wire().read(() -> "key").text(sb);
                Assert.assertEquals("value=2", sb.toString());
            }

            try (final DocumentContext dc = tailer.readingDocument()) {
                assert dc.isPresent();
                assert dc.isData();
                dc.wire().read(() -> "key").text(sb);
                Assert.assertEquals("value=3", sb.toString());
            }

            try (final DocumentContext dc = tailer.readingDocument()) {
                assert dc.isPresent();
                assert dc.isData();
                dc.wire().read(() -> "key").text(sb);
                Assert.assertEquals("value=4", sb.toString());
            }

            try (final DocumentContext dc = tailer.readingDocument()) {
                assert !dc.isPresent();
                assert !dc.isData();
                assert !dc.isMetaData();
            }
        }
    }

    // TODO Test fails if you are at Epoch.
    @Test
    public void testReadingDocumentWithFirstAMoveWithEpoch() throws IOException {

        try (final ChronicleQueue chronicle = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .rollCycle(RollCycles.HOURS)
                .epoch(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1))
                .build()) {

            final ExcerptAppender appender = chronicle.createAppender();
            long cycle = appender.cycle();

            // create 100 documents
            for (int i = 0; i < 5; i++) {
                final int j = i;
                long index = appender.writeDocument(wire -> wire.write(() -> "key").text("value=" + j));
                if (i == 2) {
                    final long cycle1 = toCycle(index);
                    Assert.assertEquals(cycle1, cycle);
                }
            }

            final ExcerptTailer tailer = chronicle.createTailer();
            tailer.moveToIndex(index(cycle, 2));

            final StringBuilder sb = Wires.acquireStringBuilder();

            try (final DocumentContext dc = tailer.readingDocument()) {
                assert dc.isPresent();
                assert dc.isData();
                dc.wire().read(() -> "key").text(sb);
                Assert.assertEquals("value=2", sb.toString());
            }

            try (final DocumentContext dc = tailer.readingDocument()) {
                assert dc.isPresent();
                assert dc.isData();
                dc.wire().read(() -> "key").text(sb);
                Assert.assertEquals("value=3", sb.toString());
            }

            try (final DocumentContext dc = tailer.readingDocument()) {
                assert dc.isPresent();
                assert dc.isData();
                dc.wire().read(() -> "key").text(sb);
                Assert.assertEquals("value=4", sb.toString());
            }

            try (final DocumentContext dc = tailer.readingDocument()) {
                assert !dc.isPresent();
                assert !dc.isData();
                assert !dc.isMetaData();
            }
        }
    }

    @Test
    public void testToEnd() throws IOException, InterruptedException {
        File tmpDir = getTmpDir();
        try (ChronicleQueue chronicle = new SingleChronicleQueueBuilder(tmpDir)
                .wireType(this.wireType)
                .rollCycle(RollCycles.HOURS)
                .build()) {
            ExcerptTailer tailer = chronicle.createTailer();

            // move to the end even though it doesn't exist yet.
            tailer.toEnd();

            try (ChronicleQueue chronicle2 = new SingleChronicleQueueBuilder(tmpDir)
                    .wireType(this.wireType)
                    .rollCycle(RollCycles.HOURS)
                    .build()) {

                ExcerptAppender append = chronicle2.createAppender();
                append.writeDocument(w -> w.write(() -> "test").text("text"));

            }
            assertTrue(tailer.readDocument(w -> w.read(() -> "test").text("text", Assert::assertEquals)));
        }
    }

    @Test
    public void testToEnd2() throws IOException {
        File tmpDir = getTmpDir();
        try (ChronicleQueue chronicle = new SingleChronicleQueueBuilder(tmpDir)
                .wireType(this.wireType)
                .build();
             ChronicleQueue chronicle2 = new SingleChronicleQueueBuilder(tmpDir)
                     .wireType(this.wireType)
                     .build()) {

            ExcerptAppender append = chronicle2.createAppender();
            append.writeDocument(w -> w.write(() -> "test").text("before text"));

            ExcerptTailer tailer = chronicle.createTailer();

            // move to the end even though it doesn't exist yet.
            tailer.toEnd();

            append.writeDocument(w -> w.write(() -> "test").text("text"));

            assertTrue(tailer.readDocument(w -> w.read(() -> "test").text("text", Assert::assertEquals)));
        }
    }

    @Test
    @Ignore("Not sure it is useful")
    public void testReadWrite() throws IOException {
        File tmpDir = getTmpDir();
        try (ChronicleQueue chronicle = new SingleChronicleQueueBuilder(tmpDir)
                .wireType(this.wireType)
                .rollCycle(RollCycles.HOURS)
                .blockSize(2 << 20)
                .build();
             ChronicleQueue chronicle2 = new SingleChronicleQueueBuilder(tmpDir)
                     .wireType(this.wireType)
                     .rollCycle(RollCycles.HOURS)
                     .blockSize(2 << 20)
                     .build()) {
            ExcerptAppender append = chronicle2.createAppender();
            for (int i = 0; i < 1000000; i++)
                append.writeDocument(w -> w.write(() -> "test - message").text("text"));

            ExcerptTailer tailer = chronicle.createTailer();
            ExcerptTailer tailer2 = chronicle.createTailer();
            ExcerptTailer tailer3 = chronicle.createTailer();
            ExcerptTailer tailer4 = chronicle.createTailer();
            for (int i = 0; i < 1_000_000; i++) {
                if (i % 10000 == 0)
                    System.gc();
                if (i % 2 == 0)
                    assertTrue(tailer2.readDocument(w -> w.read(() -> "test - message").text("text", Assert::assertEquals)));
                if (i % 3 == 0)
                    assertTrue(tailer3.readDocument(w -> w.read(() -> "test - message").text("text", Assert::assertEquals)));
                if (i % 4 == 0)
                    assertTrue(tailer4.readDocument(w -> w.read(() -> "test - message").text("text", Assert::assertEquals)));
                assertTrue(tailer.readDocument(w -> w.read(() -> "test - message").text("text", Assert::assertEquals)));
            }
        }
    }

    @Test
    @Ignore("TODO FIX")
    public void testReadingDocumentForEmptyQueue() throws IOException {
        File tmpDir = getTmpDir();
        try (ChronicleQueue chronicle = new SingleChronicleQueueBuilder(tmpDir)
                .wireType(this.wireType)
                .rollCycle(RollCycles.HOURS)
                .build()) {
            ExcerptTailer tailer = chronicle.createTailer();
            // DocumentContext is empty as we have no queue and don't know what the wire type will be.
            try (DocumentContext dc = tailer.readingDocument()) {
                assertFalse(dc.isPresent());
            }

            try (ChronicleQueue chronicle2 = new SingleChronicleQueueBuilder(tmpDir)
                    .wireType(this.wireType)
                    .build()) {
                ExcerptAppender appender = chronicle2.createAppender();
                appender.writeDocument(w -> w.write(() -> "test - message").text("text"));

                // DocumentContext should not be empty as we know what the wire type will be.
                try (DocumentContext dc = tailer.readingDocument()) {
                    assertTrue(dc.isPresent());
                    dc.wire().read(() -> "test - message").text("text", Assert::assertEquals);
                }
            }
        }
    }
}
