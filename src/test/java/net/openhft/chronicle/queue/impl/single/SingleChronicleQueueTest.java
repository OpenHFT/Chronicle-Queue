/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.wire.*;
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
import java.util.concurrent.*;

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
//                {WireType.TEXT},
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
        try (final RollingChronicleQueue queue = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .build()) {

            final ExcerptAppender appender = queue.createAppender();
            for (int i = 0; i < 10; i++) {
                final int n = i;
                appender.writeDocument(w -> w.write(TestKey.test).int32(n));
                assertEquals(n, queue.rollCycle().toSequenceNumber(appender.lastIndexAppended()));
            }
        }
    }


    @Test
    public void testWriteWithDocumentReadBytesDifferentThreads() throws IOException,
            InterruptedException {
        try (final ChronicleQueue queue = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType).build()) {


            final String expected = "some long message";


            Executors.newSingleThreadExecutor().submit(() -> {
                final ExcerptAppender appender = queue.createAppender();

                try (final DocumentContext dc = appender.writingDocument()) {
                    dc.wire().writeEventName(() -> "key").text(expected);
                }

            });

            BlockingQueue<Bytes> result = new ArrayBlockingQueue<Bytes>(10);
            Bytes b = Bytes.allocateDirect(128);

            Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
                final ExcerptTailer tailer = queue.createTailer();
                tailer.readBytes(b);
                if (b.readRemaining() == 0)
                    return;
                b.readPosition(0);
                result.add(b);
                throw new RejectedExecutionException();
            }, 1, 1, TimeUnit.MICROSECONDS);


            final Bytes poll = result.poll(10, TimeUnit.SECONDS);
            final String actual = this.wireType.apply(poll).read(() -> "key")
                    .text();
            Assert.assertEquals(expected, actual);

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
    public void testAppendAndRead() throws IOException, TimeoutException {
        try (final RollingChronicleQueue queue = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .build()) {

            final ExcerptAppender appender = queue.createAppender();
            final int cycle = appender.cycle();
            for (int i = 0; i < 10; i++) {
                final int n = i;
                appender.writeDocument(w -> w.write(TestKey.test).int32(n));
                assertEquals(n, queue.rollCycle().toSequenceNumber(appender.lastIndexAppended()));
            }

            final ExcerptTailer tailer = queue.createTailer();

            // Sequential read
            for (int i = 0; i < 10; i++) {
                final int n = i;
                assertTrue(tailer.readDocument(r -> assertEquals(n, r.read(TestKey.test).int32())));
                assertEquals(n + 1, queue.rollCycle().toSequenceNumber(tailer.index()));
            }

            // Random read
            for (int i = 0; i < 10; i++) {
                final int n = i;
                assertTrue("n: " + n, tailer.moveToIndex(queue.rollCycle().toIndex(cycle, n)));
                assertTrue("n: " + n, tailer.readDocument(r -> assertEquals(n, r.read(TestKey.test).int32())));
                assertEquals(n + 1, queue.rollCycle().toSequenceNumber(tailer.index()));
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
    public void testAppendAndReadWithRollingB() throws IOException, InterruptedException {
        SetTimeProvider stp = new SetTimeProvider();
        stp.currentTimeMillis(System.currentTimeMillis() - 3 * 86400_000L);

        File tmpDir = getTmpDir();
        try (final ChronicleQueue queue = new SingleChronicleQueueBuilder(tmpDir)
                .wireType(this.wireType)
                .timeProvider(stp)
                .build()) {

            final ExcerptAppender appender = queue.createAppender();
            appender.writeDocument(w -> w.write(TestKey.test).int32(0));
            appender.writeDocument(w -> w.write(TestKey.test2).int32(1000));
            int cycle = appender.cycle();
            for (int i = 1; i <= 5; i++) {
                stp.currentTimeMillis(stp.currentTimeMillis() + 86400_000L);
                final int n = i;
                appender.writeDocument(w -> w.write(TestKey.test).int32(n));
                assertEquals(cycle + i, appender.cycle());
                appender.writeDocument(w -> w.write(TestKey.test2).int32(n + 1000));
                assertEquals(cycle + i, appender.cycle());
            }

            /* Note this means the file has rolled
            --- !!not-ready-meta-data! #binary
            ...
             */
            assertEquals("--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: 250,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: !int 8192,\n" +
                    "    indexSpacing: 64,\n" +
                    "    index2Index: 0,\n" +
                    "    lastIndex: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 227\n" +
                    "--- !!data #binary\n" +
                    "test: 0\n" +
                    "# position: 237\n" +
                    "--- !!data #binary\n" +
                    "test2: !int 1000\n" +
                    "# position: 250\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 83885826 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: 250,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: !int 8192,\n" +
                    "    indexSpacing: 64,\n" +
                    "    index2Index: 0,\n" +
                    "    lastIndex: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 227\n" +
                    "--- !!data #binary\n" +
                    "test: 1\n" +
                    "# position: 237\n" +
                    "--- !!data #binary\n" +
                    "test2: !int 1001\n" +
                    "# position: 250\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 83885826 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: 250,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: !int 8192,\n" +
                    "    indexSpacing: 64,\n" +
                    "    index2Index: 0,\n" +
                    "    lastIndex: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 227\n" +
                    "--- !!data #binary\n" +
                    "test: 2\n" +
                    "# position: 237\n" +
                    "--- !!data #binary\n" +
                    "test2: !int 1002\n" +
                    "# position: 250\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 83885826 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: 250,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: !int 8192,\n" +
                    "    indexSpacing: 64,\n" +
                    "    index2Index: 0,\n" +
                    "    lastIndex: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 227\n" +
                    "--- !!data #binary\n" +
                    "test: 3\n" +
                    "# position: 237\n" +
                    "--- !!data #binary\n" +
                    "test2: !int 1003\n" +
                    "# position: 250\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 83885826 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: 250,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: !int 8192,\n" +
                    "    indexSpacing: 64,\n" +
                    "    index2Index: 0,\n" +
                    "    lastIndex: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 227\n" +
                    "--- !!data #binary\n" +
                    "test: 4\n" +
                    "# position: 237\n" +
                    "--- !!data #binary\n" +
                    "test2: !int 1004\n" +
                    "# position: 250\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 83885826 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: 250,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: !int 8192,\n" +
                    "    indexSpacing: 64,\n" +
                    "    index2Index: 0,\n" +
                    "    lastIndex: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 227\n" +
                    "--- !!data #binary\n" +
                    "test: 5\n" +
                    "# position: 237\n" +
                    "--- !!data #binary\n" +
                    "test2: !int 1005\n" +
                    "...\n" +
                    "# 83885826 bytes remaining\n", queue.dump());

            final ExcerptTailer tailer = queue.createTailer().toStart();
            for (int i = 0; i < 6; i++) {
                final int n = i;
                boolean condition = tailer.readDocument(r -> assertEquals(n,
                        r.read(TestKey.test).int32()));
                assertTrue("i : " + i, condition);
                assertEquals(cycle + i, tailer.cycle());

                boolean condition2 = tailer.readDocument(r -> assertEquals(n + 1000,
                        r.read(TestKey.test2).int32()));
                assertTrue("i2 : " + i, condition2);
                assertEquals(cycle + i, tailer.cycle());
            }
        }
    }

    @Test
    public void testAppendAndReadWithRollingR() throws IOException, InterruptedException {

        File tmpDir = getTmpDir();
        try (final RollingChronicleQueue queue = new SingleChronicleQueueBuilder(tmpDir)
                .wireType(this.wireType)
                .indexCount(8)
                .indexSpacing(1)
                .build()) {

            int cycle = queue.rollCycle().current(() -> System.currentTimeMillis(), 0) - 3;

            final ExcerptAppender appender = queue.createAppender();
            Wire wire = new BinaryWire(Bytes.allocateDirect(64));
            for (int i = 0; i < 6; i++) {
                long index = queue.rollCycle().toIndex(cycle + i, 0);
                wire.bytes().clear();
                wire.write(TestKey.test).int32(i);
                appender.writeBytes(index, wire.bytes());
                wire.bytes().clear();
                wire.write(TestKey.test2).int32(i + 1000);
                appender.writeBytes(index + 1, wire.bytes());
            }


            /* Note this means the file has rolled
            --- !!not-ready-meta-data! #binary
            ...
             */
            assertEquals("--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: 346,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 225,\n" +
                    "    lastIndex: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 225\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 0\n" +
                    "  # ^ used ^\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0\n" +
                    "]\n" +
                    "# position: 323\n" +
                    "--- !!data #binary\n" +
                    "test: 0\n" +
                    "# position: 333\n" +
                    "--- !!data #binary\n" +
                    "test2: !int 1000\n" +
                    "# position: 346\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 83885730 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: 346,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 225,\n" +
                    "    lastIndex: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 225\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 0\n" +
                    "  # ^ used ^\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0\n" +
                    "]\n" +
                    "# position: 323\n" +
                    "--- !!data #binary\n" +
                    "test: 1\n" +
                    "# position: 333\n" +
                    "--- !!data #binary\n" +
                    "test2: !int 1001\n" +
                    "# position: 346\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 83885730 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: 346,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 225,\n" +
                    "    lastIndex: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 225\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 0\n" +
                    "  # ^ used ^\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0\n" +
                    "]\n" +
                    "# position: 323\n" +
                    "--- !!data #binary\n" +
                    "test: 2\n" +
                    "# position: 333\n" +
                    "--- !!data #binary\n" +
                    "test2: !int 1002\n" +
                    "# position: 346\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 83885730 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: 346,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 225,\n" +
                    "    lastIndex: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 225\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 0\n" +
                    "  # ^ used ^\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0\n" +
                    "]\n" +
                    "# position: 323\n" +
                    "--- !!data #binary\n" +
                    "test: 3\n" +
                    "# position: 333\n" +
                    "--- !!data #binary\n" +
                    "test2: !int 1003\n" +
                    "# position: 346\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 83885730 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: 346,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 225,\n" +
                    "    lastIndex: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 225\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 0\n" +
                    "  # ^ used ^\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0\n" +
                    "]\n" +
                    "# position: 323\n" +
                    "--- !!data #binary\n" +
                    "test: 4\n" +
                    "# position: 333\n" +
                    "--- !!data #binary\n" +
                    "test2: !int 1004\n" +
                    "# position: 346\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 83885730 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: 346,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 225,\n" +
                    "    lastIndex: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 225\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 0\n" +
                    "  # ^ used ^\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0\n" +
                    "]\n" +
                    "# position: 323\n" +
                    "--- !!data #binary\n" +
                    "test: 5\n" +
                    "# position: 333\n" +
                    "--- !!data #binary\n" +
                    "test2: !int 1005\n" +
                    "...\n" +
                    "# 83885730 bytes remaining\n", queue.dump());

            final ExcerptTailer tailer = queue.createTailer().toStart();
            for (int i = 0; i < 6; i++) {
                final int n = i;
                boolean condition = tailer.readDocument(r -> assertEquals(n,
                        r.read(TestKey.test).int32()));
                assertTrue("i : " + i, condition);
                assertEquals(cycle + i, tailer.cycle());

                boolean condition2 = tailer.readDocument(r -> assertEquals(n + 1000,
                        r.read(TestKey.test2).int32()));
                assertTrue("i2 : " + i, condition2);
                assertEquals(cycle + i, tailer.cycle());
            }
        }
    }

    @Test
    public void testAppendAndReadAtIndex() throws IOException, TimeoutException {
        try (final RollingChronicleQueue queue = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .build()) {

            final ExcerptAppender appender = queue.createAppender();
            appender.cycle();
            for (int i = 0; i < 5; i++) {
                final int n = i;
                appender.writeDocument(w -> w.write(TestKey.test).int32(n));
                assertEquals(n, queue.rollCycle().toSequenceNumber(appender.lastIndexAppended()));
            }

            final ExcerptTailer tailer = queue.createTailer();
            for (int i = 0; i < 5; i++) {
                final long index = queue.rollCycle().toIndex(appender.cycle(), i);
                assertTrue(tailer.moveToIndex(index));

                final int n = i;
                assertTrue(tailer.readDocument(r -> assertEquals(n, queue.rollCycle().toSequenceNumber(r.read(TestKey.test)
                        .int32()))));
                assertEquals(n + 1, queue.rollCycle().toSequenceNumber(tailer.index()));
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
    public void testReadAtIndex() throws IOException, TimeoutException {
        try (final RollingChronicleQueue queue = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .indexCount(8)
                .indexSpacing(8)
                .build()) {
            final ExcerptAppender appender = queue.createAppender();

            // create 100 documents
            for (int i = 0; i < 100; i++) {
                final int j = i;
                appender.writeDocument(wire -> wire.write(() -> "key").text("value=" + j));
            }
            long lastIndex = appender.lastIndexAppended();

            final int cycle = queue.rollCycle().toCycle(lastIndex);
            assertEquals(queue.firstCycle(), cycle);
            assertEquals(queue.lastCycle(), cycle);
            final ExcerptTailer tailer = queue.createTailer();

            //   QueueDumpMain.dump(file, new PrintWriter(System.out));

            StringBuilder sb = new StringBuilder();

            for (int i : new int[]{0, 8, 7, 9, 64, 65, 66}) {
                assertTrue("i: " + i,
                        tailer.moveToIndex(
                                queue.rollCycle().toIndex(cycle, i)));
                tailer.readDocument(wire -> wire.read(() -> "key").text(sb));
                Assert.assertEquals("value=" + i, sb.toString());
            }
        }
    }

    @Ignore("long running test")
    @Test
    public void testReadAtIndex4MB() throws IOException, TimeoutException {
        try (final RollingChronicleQueue queue = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .build()) {
            final ExcerptAppender appender = queue.createAppender();

            System.out.print("Percent written=");

            // create 100 documents
            for (long i = 0; i < TIMES; i++) {
                final long j = i;
                appender.writeDocument(wire -> wire.write(() -> "key").text("value=" + j));

                if (i % (TIMES / 20) == 0) {
                    System.out.println("" + (i * 100 / TIMES) + "%, ");
                }
            }
            long lastIndex = appender.lastIndexAppended();

            final int cycle = queue.rollCycle().toCycle(lastIndex);

            final ExcerptTailer tailer = queue.createTailer();

            //   QueueDumpMain.dump(file, new PrintWriter(System.out));

            StringBuilder sb = new StringBuilder();

            for (long i = 0; i < (4L << 20L); i++) {
                tailer.moveToIndex(queue.rollCycle().toIndex(cycle, i));
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
        try (final RollingChronicleQueue queue = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .build()) {
            final ExcerptAppender appender = queue.createAppender();

            appender.writeDocument(wire -> wire.write(() -> "key").text("test"));
            Assert.assertEquals(0, queue.rollCycle().toSequenceNumber(appender.lastIndexAppended()));
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testLastWrittenIndexPerAppenderNoData() throws IOException {
        try (final ChronicleQueue chronicle = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .build()) {
            final ExcerptAppender appender = chronicle.createAppender();
            appender.lastIndexAppended();
            Assert.fail();
        }
    }

    @Test
    public void testLastIndexPerChronicle() throws IOException {
        try (final ChronicleQueue chronicle = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .build()) {

            final ExcerptAppender appender = chronicle.createAppender();

            appender.writeDocument(wire -> wire.write(() -> "key").text("test"));
            Assert.assertEquals(appender.lastIndexAppended(), chronicle.lastIndex());
        }
    }

    @Test
    public void testHeaderIndexReadAtIndex() throws IOException, TimeoutException {
        try (final RollingChronicleQueue queue = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .build()) {

            final ExcerptAppender appender = queue.createAppender();
            final int cycle = appender.cycle();
            // create 100 documents
            for (int i = 0; i < 100; i++) {
                final int j = i;
                appender.writeDocument(wire -> wire.write(() -> "key").text("value=" + j));
            }

            final ExcerptTailer tailer = queue.createTailer();
            assertTrue(tailer.moveToIndex(queue.rollCycle().toIndex(cycle, 0)));

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
                .rollCycle(RollCycles.HOURLY)
                .build()) {

            final ExcerptAppender appender = chronicle.createAppender();
            appender.writeDocument(wire -> wire.write(() -> "key").text("value=v"));
            Assert.assertTrue(appender.cycle() == 0);
        }

    }

    @Test
    public void testIndex() throws IOException, TimeoutException {
        try (final RollingChronicleQueue queue = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .rollCycle(RollCycles.HOURLY)
                .build()) {

            final ExcerptAppender appender = queue.createAppender();
            int cycle = appender.cycle();

            // create 100 documents
            for (int i = 0; i < 5; i++) {
                final int j = i;
                appender.writeDocument(wire -> wire.write(() -> "key").text("value=" + j));
                if (i == 2) {
                    final long cycle1 = queue.rollCycle().toCycle(appender.lastIndexAppended());
                    Assert.assertEquals(cycle1, cycle);
                }
            }

            final ExcerptTailer tailer = queue.createTailer();
            tailer.moveToIndex(queue.rollCycle().toIndex(cycle, 2));

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
        try (final RollingChronicleQueue queue = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .rollCycle(RollCycles.HOURLY)
                .build()) {

            final ExcerptAppender appender = queue.createAppender();
            long cycle = appender.cycle();

            // create 100 documents
            for (int i = 0; i < 5; i++) {
                final int j = i;
                appender.writeDocument(wire -> wire.write(() -> "key").text("value=" + j));
                if (i == 2) {
                    final long cycle1 = queue.rollCycle().toCycle(appender.lastIndexAppended());
                    Assert.assertEquals(cycle1, cycle);
                }
            }

            final ExcerptTailer tailer = queue.createTailer();

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
    public void testReadingDocumentWithFirstAMove() throws IOException, TimeoutException {

        try (final RollingChronicleQueue queue = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .rollCycle(RollCycles.HOURLY)
                .build()) {

            final ExcerptAppender appender = queue.createAppender();
            int cycle = appender.cycle();

            // create 100 documents
            for (int i = 0; i < 5; i++) {
                final int j = i;
                appender.writeDocument(wire -> wire.write(() -> "key").text("value=" + j));
                if (i == 2) {
                    final long cycle1 = queue.rollCycle().toCycle(appender.lastIndexAppended());
                    Assert.assertEquals(cycle1, cycle);
                }
            }

            final ExcerptTailer tailer = queue.createTailer();
            tailer.moveToIndex(queue.rollCycle().toIndex(cycle, 2));

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
    public void testReadingDocumentWithFirstAMoveWithEpoch() throws IOException, TimeoutException {

        try (final RollingChronicleQueue queue = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(this.wireType)
                .rollCycle(RollCycles.HOURLY)
                .epoch(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1))
                .build()) {

            final ExcerptAppender appender = queue.createAppender();
            int cycle = appender.cycle();

            // create 100 documents
            for (int i = 0; i < 5; i++) {
                final int j = i;
                appender.writeDocument(wire -> wire.write(() -> "key").text("value=" + j));
                if (i == 2) {
                    final long cycle1 = queue.rollCycle().toCycle(appender.lastIndexAppended());
                    Assert.assertEquals(cycle1, cycle);
                }
            }

            final ExcerptTailer tailer = queue.createTailer();
            tailer.moveToIndex(queue.rollCycle().toIndex(cycle, 2));

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
                .rollCycle(RollCycles.HOURLY)
                .build()) {
            ExcerptTailer tailer = chronicle.createTailer();

            // move to the end even though it doesn't exist yet.
            tailer.toEnd();

            try (ChronicleQueue chronicle2 = new SingleChronicleQueueBuilder(tmpDir)
                    .wireType(this.wireType)
                    .rollCycle(RollCycles.HOURLY)
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
//    @Ignore("Not sure it is useful")
    public void testReadWrite() throws IOException {
        File tmpDir = getTmpDir();
        try (ChronicleQueue chronicle = new SingleChronicleQueueBuilder(tmpDir)
                .wireType(this.wireType)
                .rollCycle(RollCycles.HOURLY)
                .blockSize(2 << 20)
                .build();
             ChronicleQueue chronicle2 = new SingleChronicleQueueBuilder(tmpDir)
                     .wireType(this.wireType)
                     .rollCycle(RollCycles.HOURLY)
                     .blockSize(2 << 20)
                     .build()) {
            ExcerptAppender append = chronicle2.createAppender();
            for (int i = 0; i < 100000; i++)
                append.writeDocument(w -> w.write(() -> "test - message").text("text"));

            ExcerptTailer tailer = chronicle.createTailer();
            ExcerptTailer tailer2 = chronicle.createTailer();
            ExcerptTailer tailer3 = chronicle.createTailer();
            ExcerptTailer tailer4 = chronicle.createTailer();
            for (int i = 0; i < 100_000; i++) {
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
    public void testReadingDocumentForEmptyQueue() throws IOException {
        File tmpDir = getTmpDir();
        try (ChronicleQueue chronicle = new SingleChronicleQueueBuilder(tmpDir)
                .wireType(this.wireType)
                .rollCycle(RollCycles.HOURLY)
                .build()) {
            ExcerptTailer tailer = chronicle.createTailer();
            // DocumentContext is empty as we have no queue and don't know what the wire type will be.
            try (DocumentContext dc = tailer.readingDocument()) {
                assertFalse(dc.isPresent());
            }

            try (ChronicleQueue chronicle2 = new SingleChronicleQueueBuilder(tmpDir)
                    .wireType(this.wireType)
                    .rollCycle(RollCycles.HOURLY)
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
