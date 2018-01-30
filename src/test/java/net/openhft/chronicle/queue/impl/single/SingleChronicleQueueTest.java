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
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.onoes.ExceptionKey;
import net.openhft.chronicle.core.onoes.LogLevel;
import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.core.util.StringUtils;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueTestBase;
import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.TailerDirection;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueExcerpts.InternalAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueExcerpts.StoreAppender;
import net.openhft.chronicle.wire.AbstractMarshallable;
import net.openhft.chronicle.wire.Demarshallable;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static net.openhft.chronicle.core.io.Closeable.closeQuietly;
import static net.openhft.chronicle.queue.RollCycles.DAILY;
import static net.openhft.chronicle.queue.RollCycles.HOURLY;
import static net.openhft.chronicle.queue.RollCycles.MINUTELY;
import static net.openhft.chronicle.queue.RollCycles.TEST2_DAILY;
import static net.openhft.chronicle.queue.RollCycles.TEST_DAILY;
import static net.openhft.chronicle.queue.RollCycles.TEST_SECONDLY;
import static net.openhft.chronicle.wire.MarshallableOut.Padding.ALWAYS;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
public class SingleChronicleQueueTest extends ChronicleQueueTestBase {

    private static final long TIMES = (4L << 20L);
    @NotNull
    protected final WireType wireType;
    protected final boolean encryption;

    // *************************************************************************
    //
    // TESTS
    //
    // *************************************************************************
    private ThreadDump threadDump;
    private Map<ExceptionKey, Integer> exceptionKeyIntegerMap;

    /**
     * @param config test config
     * @param wireType   the type of wire
     * @param encryption
     */
    public SingleChronicleQueueTest(String config, @NotNull WireType wireType, boolean encryption) {
        this.wireType = wireType;
        this.encryption = encryption;
    }

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                //  {WireType.TEXT},
                testConfiguration(WireType.BINARY, false),
                testConfiguration(WireType.BINARY_LIGHT, false),
//                {WireType.DELTA_BINARY}
//                {WireType.FIELDLESS_BINARY}
        });
    }

    @Before
    public void before() {
        threadDump = new ThreadDump();
        threadDump.ignore(StoreComponentReferenceHandler.THREAD_NAME);
        exceptionKeyIntegerMap = Jvm.recordExceptions();
    }

    @After
    public void after() {
        threadDump.assertNoNewThreads();
        // warnings are often expected
        exceptionKeyIntegerMap.entrySet().removeIf(entry -> entry.getKey().level.equals(LogLevel.WARN));
        if (Jvm.hasException(exceptionKeyIntegerMap)) {
            Jvm.dumpException(exceptionKeyIntegerMap);
            Assert.fail();
        }
        Jvm.resetExceptionHandlers();
        BytesUtil.checkRegisteredBytes();
    }

    @Test
    public void testAppend() {
        try (final RollingChronicleQueue queue =
                     builder(getTmpDir(), wireType)
                             .build()) {

            final ExcerptAppender appender = queue.acquireAppender();
            for (int i = 0; i < 10; i++) {
                final int n = i;
                appender.writeDocument(w -> w.write(TestKey.test).int32(n));
                assertEquals(n, queue.rollCycle().toSequenceNumber(appender.lastIndexAppended()));
            }

            assertThat(countEntries(queue), is(10L));
        }
    }

    @Test
    public void testRollbackOnAppend() {
        try (final RollingChronicleQueue queue =
                     builder(getTmpDir(), wireType)
                             .build()) {

            final ExcerptAppender appender = queue.acquireAppender();

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write("hello").text("world");
            }

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write("hello").text("world2");

            }

            ExcerptTailer tailer = queue.createTailer();

            try (DocumentContext dc = tailer.readingDocument()) {
                dc.wire().read("hello");
                dc.rollbackOnClose();
            }

            try (DocumentContext dc = tailer.readingDocument()) {
                Assert.assertEquals("world", dc.wire().read("hello").text());

            }

            try (DocumentContext dc = tailer.readingDocument()) {
                Assert.assertEquals("world2", dc.wire().read("hello").text());
            }

        }
    }

    @Test
    public void testWriteWithDocumentReadBytesDifferentThreads() throws InterruptedException, TimeoutException, ExecutionException {
        try (final ChronicleQueue queue = builder(getTmpDir(), wireType)
                .build()) {

            final String expected = "some long message";

            ExecutorService service1 = Executors.newSingleThreadExecutor();
            Future f = service1.submit(() -> {
                final ExcerptAppender appender = queue.acquireAppender();

                try (final DocumentContext dc = appender.writingDocument()) {
                    dc.wire().writeEventName(() -> "key").text(expected);
                }

            });

            BlockingQueue<Bytes> result = new ArrayBlockingQueue<>(10);

            ScheduledExecutorService service2 = Executors.newSingleThreadScheduledExecutor();
            service2.scheduleAtFixedRate(() -> {
                Bytes b = Bytes.elasticHeapByteBuffer(128);
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
            f.get(1, TimeUnit.SECONDS);
            service1.shutdown();
            service2.shutdown();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldBlowUpIfTryingToCreateQueueWithIncorrectRollCycle() {
        File tmpDir = getTmpDir();
        try (final ChronicleQueue queue = builder(tmpDir, wireType).rollCycle(TEST_SECONDLY).build()) {
            try (DocumentContext documentContext = queue.acquireAppender().writingDocument()) {
                documentContext.wire().write("somekey").text("somevalue");
            }
        }


        try (final ChronicleQueue recreated = builder(tmpDir, wireType).rollCycle(HOURLY).build()) {

        }
    }



    @Test
    public void shouldOverrideRollCycleOnPreCreatedAppender() {
        File tmpDir = getTmpDir();
        try (final ChronicleQueue minuteRollCycleQueue = builder(tmpDir, wireType).rollCycle(MINUTELY).build()) {

            try (final ChronicleQueue hourlyRollCycleQueue = builder(tmpDir, wireType).rollCycle(HOURLY).build()) {

                try (DocumentContext documentContext = hourlyRollCycleQueue.acquireAppender().writingDocument()) {
                    documentContext.wire().write("somekey").text("somevalue");
                }
            }

            try (DocumentContext documentContext2 = minuteRollCycleQueue.acquireAppender().writingDocument()) {
                documentContext2.wire().write("otherkey").text("othervalue");
            }
        }
    }

    @Test
    public void shouldOverrideRollCycleOnPreCreatedTailer() {

        File tmpDir = getTmpDir();
        try (final ChronicleQueue qTailer = builder(tmpDir, wireType).rollCycle(MINUTELY).build()) {
            try (final ChronicleQueue qAppender = builder(tmpDir, wireType).rollCycle(HOURLY).build()) {
                try (DocumentContext documentContext = qAppender.acquireAppender().writingDocument()) {
                    documentContext.wire().write("somekey").text("somevalue");
                }
            }
            try (DocumentContext documentContext2 = qTailer.createTailer().readingDocument()) {
                String str = documentContext2.wire().read("somekey").text();
                Assert.assertEquals("somevalue", str);
            }
        }
    }

    @Test
    public void testReadWriteHourly() {

        File tmpDir = getTmpDir();
        try (final ChronicleQueue qAppender = builder(tmpDir, wireType).rollCycle(HOURLY).build()) {

            try (DocumentContext documentContext = qAppender.acquireAppender().writingDocument()) {
                documentContext.wire().write("somekey").text("somevalue");
            }
        }

        try (final ChronicleQueue qTailer = builder(tmpDir, wireType).rollCycle(HOURLY).build()) {

            try (DocumentContext documentContext2 = qTailer.createTailer().readingDocument()) {
                String str = documentContext2.wire().read("somekey").text();
                Assert.assertEquals("somevalue", str);
            }
        }
    }



    @Test
    public void testLastWritten() throws InterruptedException {
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

        try {
            final SetTimeProvider tp = new SetTimeProvider();
            try (SingleChronicleQueue outQueue = builder(getTmpDir(), wireType).rollCycle(RollCycles.TEST_SECONDLY).sourceId(1).timeProvider(tp).build()) {
                File inQueueTmpDir = getTmpDir();
                try (SingleChronicleQueue inQueue = builder(inQueueTmpDir, wireType).rollCycle(RollCycles.TEST_SECONDLY).sourceId(2).timeProvider(tp).build()) {

                    // write some initial data to the inqueue
                    final Msg msg = inQueue.acquireAppender().methodWriterBuilder(Msg.class).recordHistory(true).build();

                    msg.msg("somedata-0");
                    assertEquals(1, inQueueTmpDir.listFiles(file -> file.getName().endsWith("cq4")).length);

                    tp.advanceMillis(1000);

                    // write data into the inQueue
                    msg.msg("somedata-1");
                    assertEquals(2, inQueueTmpDir.listFiles(file -> file.getName().endsWith("cq4")).length);

                    // read a message on the in queue and write it to the out queue
                    {
                        Msg out = outQueue.acquireAppender().methodWriterBuilder(Msg.class).recordHistory(true).build();
                        MethodReader methodReader = inQueue.createTailer().methodReader((Msg) out::msg);

                        // reads the somedata-0
                        methodReader.readOne();

                        // reads the somedata-1
                        methodReader.readOne();

                        Assert.assertFalse(methodReader.readOne());

                        tp.advanceMillis(1000);
                        // this writes the EOF marker but doesn't create a new file
                        Assert.assertFalse(methodReader.readOne());
                    }

                    assertEquals("trying to read should not create a file", 2, inQueueTmpDir.listFiles(file -> file.getName().endsWith("cq4")).length);

                    // write data into the inQueue
                    msg.msg("somedata-2");
                    assertEquals(3, inQueueTmpDir.listFiles(file -> file.getName().endsWith("cq4")).length);

                    // advance 2 cycles - we will end up with a missing file
                    tp.advanceMillis(2000);

                    msg.msg("somedata-3");
                    msg.msg("somedata-4");
                    assertEquals("Should be a missing cycle file", 4, inQueueTmpDir.listFiles(file -> file.getName().endsWith("cq4")).length);

                    AtomicReference<String> actualValue = new AtomicReference<>();

                    // check that we are able to pick up from where we left off, in other words the next read should be somedata-2
                    {
                        ExcerptTailer excerptTailer = inQueue.createTailer().afterLastWritten(outQueue);
                        MethodReader methodReader = excerptTailer.methodReader((Msg) actualValue::set);

                        methodReader.readOne();
                        Assert.assertEquals("somedata-2", actualValue.get());

                        methodReader.readOne();
                        Assert.assertEquals("somedata-3", actualValue.get());

                        methodReader.readOne();
                        Assert.assertEquals("somedata-4", actualValue.get());

                        Assert.assertFalse(methodReader.readOne());
                    }
                }
            }
        } finally {
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.SECONDS);
            executorService.shutdownNow();
        }
    }


    @Test
    public void shouldAllowDirectoryToBeDeletedWhenQueueIsClosed() throws IOException {
        final File dir = DirectoryUtils.tempDir("to-be-deleted");
        try (final SingleChronicleQueue queue =
                     builder(dir, wireType).
                             testBlockSize().build()) {
            try (final DocumentContext dc = queue.acquireAppender().writingDocument()) {
                dc.wire().write().text("foo");
            }
            try (final DocumentContext dc = queue.createTailer().readingDocument()) {
                assertEquals("foo", dc.wire().read().text());
            }

        }


        Files.walk(dir.toPath()).forEach(p -> {
            if (!Files.isDirectory(p)) {
                assertTrue(p.toString(), p.toFile().delete());
            }
        });

        assertTrue(dir.delete());
    }


    @Test
    public void testReadingLessBytesThanWritten() {
        try (final ChronicleQueue queue = builder(getTmpDir(), wireType)
                .build()) {

            final ExcerptAppender appender = queue.acquireAppender();

            final Bytes<byte[]> expected = Bytes.wrapForRead("some long message".getBytes(ISO_8859_1));
            for (int i = 0; i < 10; i++) {

                appender.writeBytes(expected);
            }

            final ExcerptTailer tailer = queue.createTailer();

            // Sequential read
            for (int i = 0; i < 10; i++) {

                Bytes b = Bytes.allocateDirect(8);

                tailer.readBytes(b);

                Assert.assertEquals(expected.readInt(0), b.readInt(0));

                b.release();
            }
        }
    }

    @Test
    public void testAppendAndRead() {
        try (final RollingChronicleQueue queue = builder(getTmpDir(), this.wireType)
                .build()) {

            final ExcerptAppender appender = queue.acquireAppender();
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
    public void testReadAndAppend() {
        try (final ChronicleQueue queue =
                     builder(getTmpDir(), this.wireType)
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
            Jvm.pause(500); //TODO: latch

            final ExcerptAppender appender = queue.acquireAppender();
            for (int i = 0; i < 2; i++) {
                final int n = i;
                appender.writeDocument(w -> w.write(TestKey.test).int32(n));
            }

            Jvm.pause(500);

            assertArrayEquals(new int[]{0, 1}, results);
        }
    }

    @Test
    public void testCheckIndexWithWritingDocument() {
        doTestCheckIndex(
                (appender, n) -> {
                    try (final DocumentContext dc = appender.writingDocument()) {
                        dc.wire().writeEventName("").object("" + n);
                    }
                });
    }

    @Test
    public void testCheckIndexWithWritingDocument2() {
        doTestCheckIndex(
                (appender, n) -> {
                    try (final DocumentContext dc = appender.writingDocument()) {
                        dc.wire().bytes().writeUtf8("Hello")
                                .writeStopBit(12345)
                                .writeStopBit(1.2) // float also supported.
                                .writeInt(1);
                    }
                });
    }

    @Test
    public void testCheckIndexWithWriteBytes() {
        doTestCheckIndex(
                (appender, n) -> appender.writeBytes(Bytes.from("Message-" + n)));
    }

    @Test
    public void testCheckIndexWithWriteBytes2() {
        doTestCheckIndex(
                (appender, n) -> appender.writeBytes(b -> b.append8bit("Message-").append(n)));
    }

    @Test
    public void testCheckIndexWithWriteBytes3() {
        doTestCheckIndex(
                (appender, n) -> appender.writeBytes(b ->
                        b.writeUtf8("Hello")
                                .writeStopBit(12345)
                                .writeStopBit(1.2) // float also supported.
                                .writeInt(1)));
    }

    @Test
    public void testCheckIndexWithWriteMap() {
        doTestCheckIndex(
                (appender, n) -> appender.writeMap(new HashMap<String, String>() {{
                    put("key", "Message-" + n);
                }}));
    }

    @Test
    public void testCheckIndexWithWriteText() {
        doTestCheckIndex(
                (appender, n) -> appender.writeText("Message-" + n)
        );
    }

    void doTestCheckIndex(@NotNull BiConsumer<ExcerptAppender, Integer> writeTo) {
        SetTimeProvider stp = new SetTimeProvider();
        stp.currentTimeMillis(System.currentTimeMillis() - 3 * 86400_000L);
        try (final ChronicleQueue queue = builder(getTmpDir(), wireType)
                .timeProvider(stp)
                .build()) {

            final ExcerptAppender appender = queue.acquireAppender();
            ExcerptTailer tailer = queue.createTailer();
            int cycle = appender.cycle();
            for (int i = 0; i <= 5; i++) {
                final int n = i;

                writeTo.accept(appender, n);
                assertEquals(cycle + i, appender.cycle());

                try (DocumentContext dc = tailer.readingDocument()) {
                    long index = tailer.index();
                    System.out.println(i + " index: " + Long.toHexString(index));
                    assertEquals(appender.cycle(), tailer.cycle());
                    assertEquals(cycle + i, DAILY.toCycle(index));
                }
                stp.currentTimeMillis(stp.currentTimeMillis() + 86400_000L);

            }
        }
    }

    @Test
    public void testAppendAndReadWithRollingB() {
        SetTimeProvider stp = new SetTimeProvider();
        stp.currentTimeMillis(System.currentTimeMillis() - 3 * 86400_000L);

        try (final ChronicleQueue queue =
                     builder(getTmpDir(), this.wireType)
                             .rollCycle(TEST_DAILY)
                             .timeProvider(stp)
                             .build()) {

            final ExcerptAppender appender = queue.acquireAppender();
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
            assertEquals(expectedAppendAndReadWithRolling(), queue.dump());

            assumeFalse(encryption);
            assumeFalse(wireType == WireType.DEFAULT_ZERO_BINARY);
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

    @NotNull
    protected String expectedAppendAndReadWithRolling() {

        if (wireType == WireType.BINARY)

            return "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: [\n" +
                    "    650,\n" +
                    "    2791728742401\n" +
                    "  ],\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 442,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  lastIndexReplicated: -1,\n" +
                    "  sourceId: 0\n" +
                    "}\n" +
                    "# position: 442, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  544,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 544, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  640,\n" +
                    "  650,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 640, header: 0\n" +
                    "--- !!data #binary\n" +
                    "test: 0\n" +
                    "# position: 650, header: 1\n" +
                    "--- !!data #binary\n" +
                    "test2: !short 1000\n" +
                    "# position: 663, header: 1 EOF\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 130405 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: [\n" +
                    "    650,\n" +
                    "    2791728742401\n" +
                    "  ],\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 442,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  lastIndexReplicated: -1,\n" +
                    "  sourceId: 0\n" +
                    "}\n" +
                    "# position: 442, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  544,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 544, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  640,\n" +
                    "  650,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 640, header: 0\n" +
                    "--- !!data #binary\n" +
                    "test: 1\n" +
                    "# position: 650, header: 1\n" +
                    "--- !!data #binary\n" +
                    "test2: !short 1001\n" +
                    "# position: 663, header: 1 EOF\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 130405 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: [\n" +
                    "    650,\n" +
                    "    2791728742401\n" +
                    "  ],\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 442,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  lastIndexReplicated: -1,\n" +
                    "  sourceId: 0\n" +
                    "}\n" +
                    "# position: 442, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  544,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 544, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  640,\n" +
                    "  650,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 640, header: 0\n" +
                    "--- !!data #binary\n" +
                    "test: 2\n" +
                    "# position: 650, header: 1\n" +
                    "--- !!data #binary\n" +
                    "test2: !short 1002\n" +
                    "# position: 663, header: 1 EOF\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 130405 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: [\n" +
                    "    650,\n" +
                    "    2791728742401\n" +
                    "  ],\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 442,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  lastIndexReplicated: -1,\n" +
                    "  sourceId: 0\n" +
                    "}\n" +
                    "# position: 442, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  544,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 544, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  640,\n" +
                    "  650,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 640, header: 0\n" +
                    "--- !!data #binary\n" +
                    "test: 3\n" +
                    "# position: 650, header: 1\n" +
                    "--- !!data #binary\n" +
                    "test2: !short 1003\n" +
                    "# position: 663, header: 1 EOF\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 130405 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: [\n" +
                    "    650,\n" +
                    "    2791728742401\n" +
                    "  ],\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 442,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  lastIndexReplicated: -1,\n" +
                    "  sourceId: 0\n" +
                    "}\n" +
                    "# position: 442, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  544,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 544, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  640,\n" +
                    "  650,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 640, header: 0\n" +
                    "--- !!data #binary\n" +
                    "test: 4\n" +
                    "# position: 650, header: 1\n" +
                    "--- !!data #binary\n" +
                    "test2: !short 1004\n" +
                    "# position: 663, header: 1 EOF\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 130405 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: [\n" +
                    "    650,\n" +
                    "    2791728742401\n" +
                    "  ],\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 442,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  lastIndexReplicated: -1,\n" +
                    "  sourceId: 0\n" +
                    "}\n" +
                    "# position: 442, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  544,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 544, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  640,\n" +
                    "  650,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 640, header: 0\n" +
                    "--- !!data #binary\n" +
                    "test: 5\n" +
                    "# position: 650, header: 1\n" +
                    "--- !!data #binary\n" +
                    "test2: !short 1005\n" +
                    "...\n" +
                    "# 130405 bytes remaining\n";

        if (wireType == WireType.BINARY_LIGHT)
            return "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: [\n" +
                    "    650,\n" +
                    "    2791728742401\n" +
                    "  ],\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 442,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  lastIndexReplicated: -1,\n" +
                    "  sourceId: 0\n" +
                    "}\n" +
                    "# position: 442, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  544,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 544, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  640,\n" +
                    "  650,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 640, header: 0\n" +
                    "--- !!data #binary\n" +
                    "test: 0\n" +
                    "# position: 650, header: 1\n" +
                    "--- !!data #binary\n" +
                    "test2: !short 1000\n" +
                    "# position: 663, header: 1 EOF\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 130405 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: [\n" +
                    "    650,\n" +
                    "    2791728742401\n" +
                    "  ],\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 442,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  lastIndexReplicated: -1,\n" +
                    "  sourceId: 0\n" +
                    "}\n" +
                    "# position: 442, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  544,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 544, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  640,\n" +
                    "  650,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 640, header: 0\n" +
                    "--- !!data #binary\n" +
                    "test: 1\n" +
                    "# position: 650, header: 1\n" +
                    "--- !!data #binary\n" +
                    "test2: !short 1001\n" +
                    "# position: 663, header: 1 EOF\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 130405 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: [\n" +
                    "    650,\n" +
                    "    2791728742401\n" +
                    "  ],\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 442,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  lastIndexReplicated: -1,\n" +
                    "  sourceId: 0\n" +
                    "}\n" +
                    "# position: 442, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  544,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 544, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  640,\n" +
                    "  650,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 640, header: 0\n" +
                    "--- !!data #binary\n" +
                    "test: 2\n" +
                    "# position: 650, header: 1\n" +
                    "--- !!data #binary\n" +
                    "test2: !short 1002\n" +
                    "# position: 663, header: 1 EOF\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 130405 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: [\n" +
                    "    650,\n" +
                    "    2791728742401\n" +
                    "  ],\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 442,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  lastIndexReplicated: -1,\n" +
                    "  sourceId: 0\n" +
                    "}\n" +
                    "# position: 442, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  544,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 544, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  640,\n" +
                    "  650,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 640, header: 0\n" +
                    "--- !!data #binary\n" +
                    "test: 3\n" +
                    "# position: 650, header: 1\n" +
                    "--- !!data #binary\n" +
                    "test2: !short 1003\n" +
                    "# position: 663, header: 1 EOF\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 130405 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: [\n" +
                    "    650,\n" +
                    "    2791728742401\n" +
                    "  ],\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 442,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  lastIndexReplicated: -1,\n" +
                    "  sourceId: 0\n" +
                    "}\n" +
                    "# position: 442, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  544,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 544, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  640,\n" +
                    "  650,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 640, header: 0\n" +
                    "--- !!data #binary\n" +
                    "test: 4\n" +
                    "# position: 650, header: 1\n" +
                    "--- !!data #binary\n" +
                    "test2: !short 1004\n" +
                    "# position: 663, header: 1 EOF\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 130405 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: [\n" +
                    "    650,\n" +
                    "    2791728742401\n" +
                    "  ],\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 442,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  lastIndexReplicated: -1,\n" +
                    "  sourceId: 0\n" +
                    "}\n" +
                    "# position: 442, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  544,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 544, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  640,\n" +
                    "  650,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 640, header: 0\n" +
                    "--- !!data #binary\n" +
                    "test: 5\n" +
                    "# position: 650, header: 1\n" +
                    "--- !!data #binary\n" +
                    "test2: !short 1005\n" +
                    "...\n" +
                    "# 130405 bytes remaining\n";

        throw new IllegalStateException("unsupported wire-type=" + wireType);
    }

    @Test
    public void testAppendAndReadAtIndex() {
        try (final RollingChronicleQueue queue = builder(getTmpDir(), this.wireType)
                .rollCycle(TEST2_DAILY)
                .build()) {

            final ExcerptAppender appender = queue.acquireAppender();
            appender.cycle();
            for (int i = 0; i < 5; i++) {
                final int n = i;
                appender.writeDocument(w -> w.write(TestKey.test).int32(n));
                assertEquals(i, queue.rollCycle().toSequenceNumber(appender.lastIndexAppended()));
            }

//            System.out.println(queue.dump());
            final ExcerptTailer tailer = queue.createTailer();
            for (int i = 0; i < 5; i++) {
                final long index = queue.rollCycle().toIndex(appender.cycle(), i);
                assertTrue(tailer.moveToIndex(index));

                final int n = i;
                assertTrue(tailer.readDocument(r -> assertEquals(n, queue.rollCycle().toSequenceNumber(r.read(TestKey.test)
                        .int32()))));
                long index2 = tailer.index();
                long sequenceNumber = queue.rollCycle().toSequenceNumber(index2);
                assertEquals(n + 1, sequenceNumber);
            }
        }
    }

    @Test
    public void testSimpleWire() {
        try (final ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .build()) {

            final ExcerptAppender appender = chronicle.acquireAppender();
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
    public void testIndexWritingDocument() {
        try (final ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .build()) {

            final ExcerptAppender appender = chronicle.acquireAppender();

            long index;
            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write(() -> "FirstName").text("Quartilla");
                index = dc.index();
            }

            try (DocumentContext dc = appender.writingDocument(true)) {
                dc.wire().write(() -> "FirstName").text("Quartilla");
            }

            Assert.assertEquals(index, appender.lastIndexAppended());
        }
    }

    @Test
    public void testReadingWritingMarshallableDocument() {
        try (final ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .build()) {

            MyMarshable myMarshable = new MyMarshable();

            final ExcerptAppender appender = chronicle.acquireAppender();

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write("myMarshable").typedMarshallable(myMarshable);
            }

            ExcerptTailer tailer = chronicle.createTailer();

            try (DocumentContext dc = tailer.readingDocument()) {

                Assert.assertEquals(myMarshable, dc.wire().read(() -> "myMarshable").typedMarshallable());
            }
        }
    }

    @Test
    public void testMetaData() {
        try (final ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .build()) {

            final ExcerptAppender appender = chronicle.acquireAppender();

            try (DocumentContext dc = appender.writingDocument(true)) {
                dc.wire().write(() -> "FirstName").text("Quartilla");
            }

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write(() -> "FirstName").text("Rob");
            }

            try (DocumentContext dc = appender.writingDocument(true)) {
                dc.wire().write(() -> "FirstName").text("Steve");
            }

            final ExcerptTailer tailer = chronicle.createTailer();

            StringBuilder event = new StringBuilder();
            while (true) {
                try (DocumentContext dc = tailer.readingDocument(true)) {
                    assertTrue(dc.isMetaData());
                    ValueIn in = dc.wire().read(event);
                    if (!StringUtils.isEqual(event, "FirstName"))
                        continue;

                    in.text("Quartilla", Assert::assertEquals);
                    break;
                }
            }

            try (DocumentContext dc = tailer.readingDocument(true)) {
                assertTrue(dc.isData());
                dc.wire().read(() -> "FirstName").text("Rob", Assert::assertEquals);
            }

            while (true) {
                try (DocumentContext dc = tailer.readingDocument(true)) {
                    assertTrue(dc.isMetaData());
                    ValueIn in = dc.wire().read(event);
                    if (!StringUtils.isEqual(event, "FirstName"))
                        continue;

                    in.text("Steve", Assert::assertEquals);
                    break;
                }
            }
        }
    }

    @Test
    public void testReadingSecondDocumentNotExist() {
        try (final ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .build()) {

            final ExcerptAppender appender = chronicle.acquireAppender();

            try (DocumentContext dc = appender.writingDocument()) {

                dc.wire().write(() -> "FirstName").text("Quartilla");
            }

            final ExcerptTailer tailer = chronicle.createTailer();

            try (DocumentContext dc = tailer.readingDocument()) {
                String text = dc.wire().read(() -> "FirstName").text();
                Assert.assertEquals("Quartilla", text);
            }

            try (DocumentContext dc = tailer.readingDocument()) {
                assertFalse(dc.isPresent());
            }
        }
    }

    @Test
    public void testDocumentIndexTest() {
        try (final SingleChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .build()) {

            final ExcerptAppender appender = chronicle.acquireAppender();

            try (DocumentContext dc = appender.writingDocument()) {
                long index = dc.index();
                Assert.assertEquals(0, chronicle.rollCycle().toSequenceNumber(index));
                dc.wire().write(() -> "FirstName").text("Quartilla");
            }

            try (DocumentContext dc = appender.writingDocument()) {
                Assert.assertEquals(1, chronicle.rollCycle().toSequenceNumber(dc.index()));
                dc.wire().write(() -> "FirstName").text("Rob");
            }

            try (DocumentContext dc = appender.writingDocument()) {
                Assert.assertEquals(2, chronicle.rollCycle().toSequenceNumber(dc.index()));
                dc.wire().write(() -> "FirstName").text("Rob");
            }

            ExcerptTailer tailer = chronicle.createTailer();

            try (DocumentContext dc = tailer.readingDocument()) {
                long index = dc.index();
                Assert.assertEquals(0, chronicle.rollCycle().toSequenceNumber(index));

            }

            try (DocumentContext dc = tailer.readingDocument()) {
                Assert.assertEquals(1, chronicle.rollCycle().toSequenceNumber(dc.index()));

            }

            try (DocumentContext dc = tailer.readingDocument()) {
                Assert.assertEquals(2, chronicle.rollCycle().toSequenceNumber(dc.index()));

            }
        }
    }

    @Test
    public void testReadingSecondDocumentNotExistIncludingMeta() {
        try (final ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .build()) {

            final ExcerptAppender appender = chronicle.acquireAppender();

            try (DocumentContext dc = appender.writingDocument()) {

                dc.wire().write(() -> "FirstName").text("Quartilla");
            }

            final ExcerptTailer tailer = chronicle.createTailer();
            StringBuilder event = new StringBuilder();
            while (true) {
                try (DocumentContext dc = tailer.readingDocument(true)) {

                    ValueIn in = dc.wire().read(event);
                    if (!StringUtils.isEqual(event, "FirstName"))
                        continue;

                    in.text("Quartilla", Assert::assertEquals);
                    break;
                }
            }

            try (DocumentContext dc = tailer.readingDocument()) {
                assertFalse(dc.isPresent());
            }
        }
    }

    @Test
    public void testSimpleByteTest() {
        try (final ChronicleQueue chronicle = builder(getTmpDir(), wireType)
                .rollCycle(TEST2_DAILY)
                .build()) {

            final ExcerptAppender appender = chronicle.acquireAppender();
            Bytes steve = Bytes.allocateDirect("Steve".getBytes());
            appender.writeBytes(steve);
            Bytes jobs = Bytes.allocateDirect("Jobs".getBytes());
            appender.writeBytes(jobs);

//            System.out.println(chronicle.dump());

            final ExcerptTailer tailer = chronicle.createTailer();
            Bytes bytes = Bytes.elasticByteBuffer();
            tailer.readBytes(bytes);
            Assert.assertEquals("Steve", bytes.toString());
            tailer.readBytes(bytes);
            Assert.assertEquals("Jobs", bytes.toString());

            steve.release();
            jobs.release();
            bytes.release();
        }
    }

    @Test
    public void testReadAtIndex() {
        try (final RollingChronicleQueue queue = builder(getTmpDir(), wireType)
                .indexCount(8)
                .indexSpacing(8)
                .build()) {
            final ExcerptAppender appender = queue.acquireAppender();

            // create 100 documents
            for (int i = 0; i < 100; i++) {
                final int j = i;
                try (final DocumentContext context = appender.writingDocument()) {
                    context.wire().write(() -> "key").text("value=" + j);
                }
            }
            long lastIndex = appender.lastIndexAppended();

            final int cycle = queue.rollCycle().toCycle(lastIndex);
            assertEquals(queue.firstCycle(), cycle);
            assertEquals(queue.lastCycle(), cycle);
            final ExcerptTailer tailer = queue.createTailer();

            StringBuilder sb = new StringBuilder();

            for (int i : new int[]{0, 8, 7, 9, 64, 65, 66}) {
                final long index = queue.rollCycle().toIndex(cycle, i);
                assertTrue("i: " + i,
                        tailer.moveToIndex(
                                index));
                final DocumentContext context = tailer.readingDocument();
                assertThat(context.index(), is(index));
                context.wire().read(() -> "key").text(sb);
                Assert.assertEquals("value=" + i, sb.toString());
            }
        }
    }

    @Ignore("long running test")
    @Test
    public void testReadAtIndex4MB() {
        try (final RollingChronicleQueue queue = builder(getTmpDir(), this.wireType)
                .build()) {
            final ExcerptAppender appender = queue.acquireAppender();

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
                assertTrue(tailer.moveToIndex(queue.rollCycle().toIndex(cycle, i)));
                tailer.readDocument(wire -> wire.read(() -> "key").text(sb));
                Assert.assertEquals("value=" + i, sb.toString());
                if (i % (TIMES / 20) == 0) {
                    System.out.println("Percent read= " + (i * 100 / TIMES) + "%");
                }
            }
        }
    }

    @Test
    public void testLastWrittenIndexPerAppender() {
        try (final RollingChronicleQueue queue = builder(getTmpDir(), this.wireType)
                .build()) {
            final ExcerptAppender appender = queue.acquireAppender();

            appender.writeDocument(wire -> wire.write(() -> "key").text("test"));
            Assert.assertEquals(0, queue.rollCycle().toSequenceNumber(appender.lastIndexAppended()));
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testLastWrittenIndexPerAppenderNoData() {
        try (final ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .build()) {
            final ExcerptAppender appender = chronicle.acquireAppender();
            appender.lastIndexAppended();
            Assert.fail();
        }
    }

    @Test(expected = IllegalStateException.class) //: no messages written
    public void testNoMessagesWritten() {
        try (final ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .build()) {

            final ExcerptAppender appender = chronicle.acquireAppender();
            appender.lastIndexAppended();
        }
    }

    @Test
    public void testHeaderIndexReadAtIndex() {
        try (final RollingChronicleQueue queue = builder(getTmpDir(), this.wireType)
                .build()) {

            final ExcerptAppender appender = queue.acquireAppender();
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
     * @
     */
    @Test
    public void testEPOC() {
        try (final ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .epoch(System.currentTimeMillis())
                .rollCycle(RollCycles.HOURLY)
                .build()) {

            final ExcerptAppender appender = chronicle.acquireAppender();
            appender.writeDocument(wire -> wire.write(() -> "key").text("value=v"));
            Assert.assertTrue(appender.cycle() == 0);
        }
    }

    @Test
    public void shouldBeAbleToReadFromQueueWithNonZeroEpoch() {
        try (final ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .epoch(System.currentTimeMillis())
                .rollCycle(RollCycles.DAILY)
                .build()) {

            final ExcerptAppender appender = chronicle.acquireAppender();
            appender.writeDocument(wire -> wire.write(() -> "key").text("value=v"));
            Assert.assertTrue(appender.cycle() == 0);

            final ExcerptTailer excerptTailer = chronicle.createTailer().toStart();
            assertThat(excerptTailer.readingDocument().isPresent(), is(true));
        }
    }

    @Test
    public void shouldHandleLargeEpoch() {
        try (final ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .epoch(System.currentTimeMillis())
                .epoch(1284739200000L)
                .rollCycle(DAILY)
                .build()) {

            final ExcerptAppender appender = chronicle.acquireAppender();
            appender.writeDocument(wire -> wire.write(() -> "key").text("value=v"));

            final ExcerptTailer excerptTailer = chronicle.createTailer().toStart();
            assertThat(excerptTailer.readingDocument().isPresent(), is(true));
        }
    }

    @Test
    public void testNegativeEPOC() {
        for (int h = -14; h <= 14; h++) {
            try (final ChronicleQueue chronicle = builder(getTmpDir(), wireType)
                    .epoch(TimeUnit.HOURS.toMillis(h))
                    .build()) {

                final ExcerptAppender appender = chronicle.acquireAppender();
                appender.writeDocument(wire -> wire.write(() -> "key").text("value=v"));
                chronicle.createTailer()
                        .readDocument(wire -> {
                            assertEquals("value=v", wire.read("key").text());
                        });
            }
        }
    }

    @Test
    public void testIndex() {
        try (final RollingChronicleQueue queue = builder(getTmpDir(), this.wireType)
                .rollCycle(RollCycles.HOURLY)
                .build()) {

            final ExcerptAppender appender = queue.acquireAppender();
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
            assertTrue(tailer.moveToIndex(queue.rollCycle().toIndex(cycle, 2)));

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
    public void testReadingDocument() {
        try (final RollingChronicleQueue queue = builder(getTmpDir(), this.wireType)
                .rollCycle(RollCycles.HOURLY)
                .build()) {

            final ExcerptAppender appender = queue.acquireAppender();
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
    public void testReadingDocumentWithFirstAMove() {

        try (final RollingChronicleQueue queue = builder(getTmpDir(), this.wireType)
                .rollCycle(RollCycles.HOURLY)
                .build()) {

            final ExcerptAppender appender = queue.acquireAppender();
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
            assertTrue(tailer.moveToIndex(queue.rollCycle().toIndex(cycle, 2)));

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
    public void testReadingDocumentWithFirstAMoveWithEpoch() {

        try (final RollingChronicleQueue queue = builder(getTmpDir(), this.wireType)
                .rollCycle(RollCycles.HOURLY)
                .epoch(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1))
                .build()) {

            final ExcerptAppender appender = queue.acquireAppender();
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
            assertTrue(tailer.moveToIndex(queue.rollCycle().toIndex(cycle, 2)));

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
    public void testToEnd() {
        File dir = getTmpDir();
        try (ChronicleQueue chronicle = builder(dir, wireType)
                .rollCycle(RollCycles.HOURLY)
                .build()) {
            ExcerptTailer tailer = chronicle.createTailer();

            // move to the end even though it doesn't exist yet.
            tailer.toEnd();

            try (ChronicleQueue chronicle2 = builder(dir, wireType)
                    .rollCycle(RollCycles.HOURLY)
                    .build()) {

                ExcerptAppender append = chronicle2.acquireAppender();
                append.writeDocument(w -> w.write(() -> "test").text("text"));

            }
            assertTrue(tailer.readDocument(w -> w.read(() -> "test").text("text", Assert::assertEquals)));
        }
    }

    @Test
    public void testAppendedBeforeToEnd() {
        File dir = getTmpDir();
        try (ChronicleQueue chronicle = builder(dir, this.wireType)
                .rollCycle(RollCycles.TEST_SECONDLY)
                .build();

             ChronicleQueue chronicle2 = builder(dir, this.wireType)
                     .rollCycle(RollCycles.TEST_SECONDLY)
                     .build()) {
            ExcerptTailer tailer = chronicle.createTailer();

            ExcerptAppender append = chronicle2.acquireAppender();
            append.writeDocument(w -> w.write(() -> "test").text("text"));

            tailer.toEnd();
            try (DocumentContext dc = tailer.readingDocument()) {
                assertFalse(dc.isPresent());
            }

            append.writeDocument(w -> w.write(() -> "test").text("text2"));
            assertTrue(tailer.readDocument(w -> w.read(() -> "test").text("text2", Assert::assertEquals)));
        }
    }

    @Test
    public void testToEnd2() {
        File dir = getTmpDir();
        try (ChronicleQueue chronicle = builder(dir, wireType)
                .build();
             ChronicleQueue chronicle2 = builder(dir, wireType)
                     .build()) {

            ExcerptAppender append = chronicle2.acquireAppender();
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
    public void testReadWrite() {
        File dir = getTmpDir();
        try (ChronicleQueue chronicle = builder(dir, wireType)
                .rollCycle(RollCycles.HOURLY)
                .blockSize(2 << 20)
                .build();
             ChronicleQueue chronicle2 = builder(dir, wireType)
                     .rollCycle(RollCycles.HOURLY)
                     .blockSize(2 << 20)
                     .build()) {
            ExcerptAppender append = chronicle2.acquireAppender();
            for (int i = 0; i < 50_000; i++)
                append.writeDocument(w -> w.write(() -> "test - message").text("text"));

            ExcerptTailer tailer = chronicle.createTailer();
            ExcerptTailer tailer2 = chronicle.createTailer();
            ExcerptTailer tailer3 = chronicle.createTailer();
            ExcerptTailer tailer4 = chronicle.createTailer();
            for (int i = 0; i < 50_000; i++) {
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
    public void testReadingDocumentForEmptyQueue() {
        File dir = getTmpDir();
        try (ChronicleQueue chronicle = builder(dir, this.wireType)
                .rollCycle(RollCycles.HOURLY)
                .build()) {
            ExcerptTailer tailer = chronicle.createTailer();
            // DocumentContext is empty as we have no queue and don't know what the wire type will be.
            try (DocumentContext dc = tailer.readingDocument()) {
                assertFalse(dc.isPresent());
            }

            try (ChronicleQueue chronicle2 = builder(dir, this.wireType)
                    .rollCycle(RollCycles.HOURLY)
                    .build()) {
                ExcerptAppender appender = chronicle2.acquireAppender();
                appender.writeDocument(w -> w.write(() -> "test - message").text("text"));

                // DocumentContext should not be empty as we know what the wire type will be.
                try (DocumentContext dc = tailer.readingDocument()) {
                    assertTrue(dc.isPresent());
                    dc.wire().read(() -> "test - message").text("text", Assert::assertEquals);
                }
            }
        }
    }

    @Test
    public void testMetaData6() {
        try (final ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .rollCycle(TEST2_DAILY)
                .build()) {

            final ExcerptAppender appender = chronicle.acquireAppender();

            try (DocumentContext dc = appender.writingDocument(true)) {
                dc.wire().write(() -> "FirstName").text("Quartilla");
            }

            try (DocumentContext dc = appender.writingDocument()) {
                assertFalse(dc.isMetaData());
                dc.wire().write(() -> "FirstName").text("Helen");
            }
            try (DocumentContext dc = appender.writingDocument(true)) {
                dc.wire().write(() -> "FirstName").text("Steve");
            }

            assertEquals(expectedMetaDataTest2(), chronicle.dump());
            final ExcerptTailer tailer = chronicle.createTailer();

            StringBuilder event = new StringBuilder();
            while (true) {
                try (DocumentContext dc = tailer.readingDocument(true)) {
                    assertTrue(dc.isMetaData());
                    ValueIn in = dc.wire().read(event);
                    if (!StringUtils.isEqual(event, "FirstName"))
                        continue;

                    in.text("Quartilla", Assert::assertEquals);
                    break;
                }
            }

            try (DocumentContext dc = tailer.readingDocument(true)) {
                assertTrue(dc.isData());
                assertTrue(dc.isPresent());
                dc.wire().read(() -> "FirstName").text("Helen", Assert::assertEquals);
            }

            while (true) {
                try (DocumentContext dc = tailer.readingDocument(true)) {
                    assertTrue(dc.isMetaData());
                    ValueIn in = dc.wire().read(event);
                    if (!StringUtils.isEqual(event, "FirstName"))
                        continue;

                    in.text("Steve", Assert::assertEquals);
                    break;
                }
            }
        }
    }

    @NotNull
    protected String expectedMetaData6() {

        if (wireType == WireType.BINARY)
            return "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: 578,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 369,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0\n" +
                    "}\n" +
                    "# position: 369, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  472,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 472, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  568,\n" +
                    "  578,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 568, header: 0\n" +
                    "--- !!data #binary\n" +
                    "test: 0\n" +
                    "# position: 578, header: 1\n" +
                    "--- !!data #binary\n" +
                    "test2: !short 1000\n" +
                    "# position: 591, header: 1 EOF\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 327085 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: 578,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 369,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0\n" +
                    "}\n" +
                    "# position: 369, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  472,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 472, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  568,\n" +
                    "  578,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 568, header: 0\n" +
                    "--- !!data #binary\n" +
                    "test: 1\n" +
                    "# position: 578, header: 1\n" +
                    "--- !!data #binary\n" +
                    "test2: !short 1001\n" +
                    "# position: 591, header: 1 EOF\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 327085 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: 578,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 369,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0\n" +
                    "}\n" +
                    "# position: 369, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  472,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 472, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  568,\n" +
                    "  578,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 568, header: 0\n" +
                    "--- !!data #binary\n" +
                    "test: 2\n" +
                    "# position: 578, header: 1\n" +
                    "--- !!data #binary\n" +
                    "test2: !short 1002\n" +
                    "# position: 591, header: 1 EOF\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 327085 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: 578,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 369,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0\n" +
                    "}\n" +
                    "# position: 369, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  472,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 472, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  568,\n" +
                    "  578,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 568, header: 0\n" +
                    "--- !!data #binary\n" +
                    "test: 3\n" +
                    "# position: 578, header: 1\n" +
                    "--- !!data #binary\n" +
                    "test2: !short 1003\n" +
                    "# position: 591, header: 1 EOF\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 327085 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: 578,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 369,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0\n" +
                    "}\n" +
                    "# position: 369, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  472,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 472, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  568,\n" +
                    "  578,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 568, header: 0\n" +
                    "--- !!data #binary\n" +
                    "test: 4\n" +
                    "# position: 578, header: 1\n" +
                    "--- !!data #binary\n" +
                    "test2: !short 1004\n" +
                    "# position: 591, header: 1 EOF\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 327085 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: 578,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 369,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0\n" +
                    "}\n" +
                    "# position: 369, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  472,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 472, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  568,\n" +
                    "  578,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 568, header: 0\n" +
                    "--- !!data #binary\n" +
                    "test: 5\n" +
                    "# position: 578, header: 1\n" +
                    "--- !!data #binary\n" +
                    "test2: !short 1005\n" +
                    "...\n" +
                    "# 327085 bytes remaining\n";

        if (wireType == WireType.BINARY_LIGHT)
            return "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 728,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 16,\n" +
                    "    indexSpacing: 2,\n" +
                    "    index2Index: 377,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0\n" +
                    "}\n" +
                    "# position: 377, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 16, used: 1\n" +
                    "  544,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 544, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 16, used: 1\n" +
                    "  728,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 704, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "FirstName: Quartilla\n" +
                    "# position: 728, header: 0\n" +
                    "--- !!data #binary\n" +
                    "FirstName: Helen\n" +
                    "# position: 748, header: 0\n" +
                    "--- !!meta-data #binary\n" +
                    "FirstName: Steve\n" +
                    "...\n" +
                    "# 130300 bytes remaining\n";

        throw new IllegalStateException("unknown type");
    }

    @NotNull
    protected String expectedMetaDataTest2() {

        if (wireType == WireType.BINARY)
            return "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: [\n" +
                    "    792,\n" +
                    "    3401614098432\n" +
                    "  ],\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 16,\n" +
                    "    indexSpacing: 2,\n" +
                    "    index2Index: 442,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  lastIndexReplicated: -1,\n" +
                    "  sourceId: 0\n" +
                    "}\n" +
                    "# position: 442, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 16, used: 1\n" +
                    "  608,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 608, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 16, used: 1\n" +
                    "  792,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 768, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "FirstName: Quartilla\n" +
                    "# position: 792, header: 0\n" +
                    "--- !!data #binary\n" +
                    "FirstName: Helen\n" +
                    "# position: 812, header: 0\n" +
                    "--- !!meta-data #binary\n" +
                    "FirstName: Steve\n" +
                    "...\n" +
                    "# 130236 bytes remaining\n";

        if (wireType == WireType.BINARY_LIGHT) {
            if (encryption)
                return "--- !!meta-data #binary\n" +
                        "header: !SCQStore {\n" +
                        "  wireType: !WireType BINARY_LIGHT,\n" +
                        "  writePosition: [\n" +
                        "    792,\n" +
                        "    3401614098432\n" +
                        "  ],\n" +
                        "  roll: !SCQSRoll {\n" +
                        "    length: !int 86400000,\n" +
                        "    format: yyyyMMdd,\n" +
                        "    epoch: 0\n" +
                        "  },\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 16,\n" +
                        "    indexSpacing: 2,\n" +
                        "    index2Index: 442,\n" +
                        "    lastIndex: 2\n" +
                        "  },\n" +
                        "  lastAcknowledgedIndexReplicated: -1,\n" +
                        "  recovery: !TimedStoreRecovery {\n" +
                        "    timeStamp: 0\n" +
                        "  },\n" +
                        "  deltaCheckpointInterval: 0,\n" +
                        "  lastIndexReplicated: -1,\n" +
                        "  sourceId: 0\n" +
                        "}\n" +
                        "# position: 442, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index2index: [\n" +
                        "  # length: 16, used: 1\n" +
                        "  608,\n" +
                        "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 608, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 16, used: 1\n" +
                        "  792,\n" +
                        "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 768, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "FirstName: Quartilla\n" +
                        "# position: 792, header: 0\n" +
                        "--- !!data #binary\n" +
                        "FirstName: Helen\n" +
                        "# position: 812, header: 0\n" +
                        "--- !!meta-data #binary\n" +
                        "FirstName: Steve\n" +
                        "...\n" +
                        "# 130236 bytes remaining\n";

            else
                return "--- !!meta-data #binary\n" +
                        "header: !SCQStore {\n" +
                        "  wireType: !WireType BINARY_LIGHT,\n" +
                        "  writePosition: [\n" +
                        "    792,\n" +
                        "    3401614098432\n" +
                        "  ],\n" +
                        "  roll: !SCQSRoll {\n" +
                        "    length: !int 86400000,\n" +
                        "    format: yyyyMMdd,\n" +
                        "    epoch: 0\n" +
                        "  },\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 16,\n" +
                        "    indexSpacing: 2,\n" +
                        "    index2Index: 442,\n" +
                        "    lastIndex: 2\n" +
                        "  },\n" +
                        "  lastAcknowledgedIndexReplicated: -1,\n" +
                        "  recovery: !TimedStoreRecovery {\n" +
                        "    timeStamp: 0\n" +
                        "  },\n" +
                        "  deltaCheckpointInterval: 0,\n" +
                        "  lastIndexReplicated: -1,\n" +
                        "  sourceId: 0\n" +
                        "}\n" +
                        "# position: 442, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index2index: [\n" +
                        "  # length: 16, used: 1\n" +
                        "  608,\n" +
                        "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 608, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 16, used: 1\n" +
                        "  792,\n" +
                        "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 768, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "FirstName: Quartilla\n" +
                        "# position: 792, header: 0\n" +
                        "--- !!data #binary\n" +
                        "FirstName: Helen\n" +
                        "# position: 812, header: 0\n" +
                        "--- !!meta-data #binary\n" +
                        "FirstName: Steve\n" +
                        "...\n" +
                        "# 130236 bytes remaining\n";

        } else if (wireType == WireType.DEFAULT_ZERO_BINARY) {
            return "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType DEFAULT_ZERO_BINARY,\n" +
                    "  writePosition: 704,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 16,\n" +
                    "    indexSpacing: 2,\n" +
                    "    index2Index: 352,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 352, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 16, used: 1\n" +
                    "  520,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 520, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 16, used: 1\n" +
                    "  704,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 680, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "FirstName: Quartilla\n" +
                    "# position: 704, header: 0\n" +
                    "--- !!data #binary\n" +
                    "FirstName: Helen\n" +
                    "# position: 724, header: 0\n" +
                    "--- !!meta-data #binary\n" +
                    "FirstName: Steve\n" +
                    "...\n" +
                    "# 326932 bytes remaining\n";

        }
        throw new IllegalStateException("unknown type " + wireType);
    }

    @Test(expected = IllegalArgumentException.class)
    public void dontPassQueueToReader() {
        try (ChronicleQueue queue = binary(getTmpDir()).build()) {
            queue.createTailer().afterLastWritten(queue).methodReader();
        }
    }

    @Test
    public void testToEndBeforeWrite() {
        try (RollingChronicleQueue chronicle = builder(getTmpDir(), wireType)
                .rollCycle(TEST2_DAILY)
                .build()) {

            ExcerptAppender appender = chronicle.acquireAppender();
            ExcerptTailer tailer = chronicle.createTailer();

            int entries = chronicle.rollCycle().defaultIndexSpacing() * 2 + 2;

            for (int i = 0; i < entries; i++) {
                tailer.toEnd();
                int finalI = i;
                appender.writeDocument(w -> w.writeEventName("hello").text("world" + finalI));
                tailer.readDocument(w -> w.read().text("world" + finalI, Assert::assertEquals));
            }
        }
    }

    @Test
    public void testSomeMessages() {
        try (RollingChronicleQueue chronicle = builder(getTmpDir(), wireType)
                .rollCycle(TEST2_DAILY)
                .build()) {

            ExcerptAppender appender = chronicle.acquireAppender();
            ExcerptTailer tailer = chronicle.createTailer();

            int entries = chronicle.rollCycle().defaultIndexSpacing() * 2 + 2;

            for (long i = 0; i < entries; i++) {
                long finalI = i;
                appender.writeDocument(w -> w.writeEventName("hello").int64(finalI));
                long seq = chronicle.rollCycle().toSequenceNumber(appender.lastIndexAppended());
                assertEquals(i, seq);
                //      System.out.println(chronicle.dump());
                tailer.readDocument(w -> w.read().int64(finalI, (a, b) -> Assert.assertEquals((long) a, b)));
            }
        }
    }

    @Test
    public void testForwardFollowedBackBackwardTailer() {
        try (RollingChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .rollCycle(TEST2_DAILY)
                .build()) {

            ExcerptAppender appender = chronicle.acquireAppender();

            int entries = chronicle.rollCycle().defaultIndexSpacing() + 2;

            for (int i = 0; i < entries; i++) {
                int finalI = i;
                appender.writeDocument(w -> w.writeEventName("hello").text("world" + finalI));
            }
            for (int i = 0; i < 3; i++) {
                readForward(chronicle, entries);
                readBackward(chronicle, entries);
            }
        }
    }

    @Test
    public void shouldReadBackwardFromEndOfQueueWhenDirectionIsSetAfterMoveToEnd() {
        assumeTrue(wireType == WireType.BINARY);
        try (final ChronicleQueue queue = builder(getTmpDir(), this.wireType)
                .rollCycle(TEST2_DAILY)
                .build()) {

            final ExcerptAppender appender = queue.acquireAppender();
            appender.writeDocument(w -> w.writeEventName("hello").text("world"));

            final ExcerptTailer tailer = queue.createTailer();
            tailer.toEnd();
            tailer.direction(TailerDirection.BACKWARD);

            assertThat(tailer.readingDocument().isPresent(), is(true));
        }
    }

    void readForward(@NotNull ChronicleQueue chronicle, int entries) {
        ExcerptTailer forwardTailer = chronicle.createTailer()
                .direction(TailerDirection.FORWARD)
                .toStart();

        for (int i = 0; i < entries; i++) {
            try (DocumentContext documentContext = forwardTailer.readingDocument()) {
                Assert.assertTrue(documentContext.isPresent());
                Assert.assertEquals(i, RollCycles.DAILY.toSequenceNumber(documentContext.index()));
                StringBuilder sb = Wires.acquireStringBuilder();
                ValueIn valueIn = documentContext.wire().readEventName(sb);
                Assert.assertTrue("hello".contentEquals(sb));
                String actual = valueIn.text();
                Assert.assertEquals("world" + i, actual);
            }
        }
        try (DocumentContext documentContext = forwardTailer.readingDocument()) {
            Assert.assertFalse(documentContext.isPresent());
        }
    }

    void readBackward(@NotNull ChronicleQueue chronicle, int entries) {
        ExcerptTailer backwardTailer = chronicle.createTailer()
                .direction(TailerDirection.BACKWARD)
                .toEnd();

        for (int i = entries - 1; i >= 0; i--) {
            try (DocumentContext documentContext = backwardTailer.readingDocument()) {
                assertTrue(documentContext.isPresent());
                final long index = documentContext.index();
                assertEquals("index: " + index, i, (int) index);
                Assert.assertEquals(i, RollCycles.DAILY.toSequenceNumber(index));
                Assert.assertTrue(documentContext.isPresent());
                StringBuilder sb = Wires.acquireStringBuilder();
                ValueIn valueIn = documentContext.wire().readEventName(sb);
                Assert.assertTrue("hello".contentEquals(sb));
                String actual = valueIn.text();
                Assert.assertEquals("world" + i, actual);
            }
        }
        try (DocumentContext documentContext = backwardTailer.readingDocument()) {
            assertFalse(documentContext.isPresent());
        }
    }

    @Test
    public void testOverreadForwardFromFutureCycleThenReadBackwardTailer() {
        RollCycle cycle = TEST2_DAILY;
        // when "forwardToFuture" flag is set, go one cycle to the future
        AtomicBoolean forwardToFuture = new AtomicBoolean(false);
        TimeProvider timeProvider = () -> forwardToFuture.get()
                ? System.currentTimeMillis() + TimeUnit.MILLISECONDS.toDays(1)
                : System.currentTimeMillis();

        try (RollingChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .rollCycle(cycle)
                .timeProvider(timeProvider)
                .build()) {

            ExcerptAppender appender = chronicle.acquireAppender();
            appender.writeDocument(w -> w.writeEventName("hello").text("world"));

            // go to the cycle next to the one the write was made on
            forwardToFuture.set(true);

            ExcerptTailer forwardTailer = chronicle.createTailer()
                    .direction(TailerDirection.FORWARD)
                    .toStart();

            try (DocumentContext context = forwardTailer.readingDocument()) {
                assertTrue(context.isPresent());
            }
            try (DocumentContext context = forwardTailer.readingDocument()) {
                assertFalse(context.isPresent());
            }

            ExcerptTailer backwardTailer = chronicle.createTailer()
                    .direction(TailerDirection.BACKWARD)
                    .toEnd();

            try (DocumentContext context = backwardTailer.readingDocument()) {
                assertTrue(context.isPresent());
            }
        }
    }

    @Test
    public void testLastIndexAppended() {
        try (ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .build()) {

            ExcerptAppender appender = chronicle.acquireAppender();
            appender.writeDocument(w -> w.writeEventName("hello").text("world0"));
            final long nextIndexToWrite = appender.lastIndexAppended() + 1;
            appender.writeDocument(w -> w.getValueOut().bytes(new byte[0]));
            //            System.out.println(chronicle.dump());
            Assert.assertEquals(nextIndexToWrite,
                    appender.lastIndexAppended());
        }
    }

    @Test
    public void testZeroLengthMessage() {
        try (ChronicleQueue chronicle = builder(getTmpDir(), wireType)
                .rollCycle(TEST_DAILY)
                .build()) {

            ExcerptAppender appender = chronicle.acquireAppender();
            appender.writeDocument(w -> {
            });
            System.out.println(chronicle.dump());
            ExcerptTailer tailer = chronicle.createTailer();
            try (DocumentContext dc = tailer.readingDocument()) {
                assertFalse(dc.wire().hasMore());
            }
        }
    }

    @Test
    public void testMoveToWithAppender() {
        try (ChronicleQueue syncQ = builder(getTmpDir(), this.wireType)
                .build()) {

            InternalAppender sync = (InternalAppender) syncQ.acquireAppender();
            File name2 = DirectoryUtils.tempDir(testName.getMethodName());
            try (ChronicleQueue chronicle = builder(name2, this.wireType)
                    .build()) {

                ExcerptAppender appender = chronicle.acquireAppender();
                appender.writeDocument(w -> w.writeEventName("hello").text("world0"));
                appender.writeDocument(w -> w.writeEventName("hello").text("world1"));
                appender.writeDocument(w -> w.writeEventName("hello").text("world2"));

                ExcerptTailer tailer = chronicle.createTailer();

                try (DocumentContext documentContext = tailer.readingDocument()) {
                    sync.writeBytes(documentContext.index(), documentContext.wire().bytes());
                }
                try (DocumentContext documentContext = tailer.readingDocument()) {
                    String text = documentContext.wire().read().text();
                    Assert.assertEquals("world1", text);
                }
            }
        }
    }

    @Test
    public void testMapWrapper() {
        try (ChronicleQueue syncQ = builder(getTmpDir(), this.wireType)
                .build()) {

            File name2 = DirectoryUtils.tempDir(testName.getMethodName());
            try (ChronicleQueue chronicle = builder(name2, this.wireType)
                    .build()) {

                ExcerptAppender appender = chronicle.acquireAppender();

                MapWrapper myMap = new MapWrapper();
                myMap.map.put("hello", 1.2);

                appender.writeDocument(w -> w.write().object(myMap));

                ExcerptTailer tailer = chronicle.createTailer();

                try (DocumentContext documentContext = tailer.readingDocument()) {
                    MapWrapper object = documentContext.wire().read().object(MapWrapper.class);
                    Assert.assertEquals(1.2, object.map.get("hello"), 0.0);
                }
            }
        }
    }

    /**
     * if one appender if much further ahead than the other, then the new append should jump
     * straight to the end rather than attempting to write a positions that are already occupied
     */
    @Test
    public void testAppendedSkipToEnd() {

        try (RollingChronicleQueue q = builder(getTmpDir(), this.wireType)
                .build()) {

            ExcerptAppender appender = q.acquireAppender();
            ExcerptAppender appender2 = q.acquireAppender();
            int indexCount = 100;

            for (int i = 0; i < indexCount; i++) {
                try (DocumentContext dc = appender.writingDocument()) {
                    dc.wire().write("key").text("some more " + 1);
                    Assert.assertEquals(i, q.rollCycle().toSequenceNumber(dc.index()));
                }
            }

            try (DocumentContext dc = appender2.writingDocument()) {
                dc.wire().write("key").text("some data " + indexCount);
                Assert.assertEquals(indexCount, q.rollCycle().toSequenceNumber(dc.index()));
            }
        }
    }

    @Test
    public void testAppendedSkipToEndMultiThreaded() {

        // some text to simulate load.
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 5; i++) sb.append(UUID.randomUUID());
        String text = sb.toString();

        try (SingleChronicleQueue q = builder(getTmpDir(), this.wireType)
                .rollCycle(TEST_SECONDLY)
                .build()) {

            final ThreadLocal<ExcerptAppender> tl = ThreadLocal.withInitial(q::acquireAppender);

            int size = 50_000;

            IntStream.range(0, size).parallel().forEach(i -> writeTestDocument(tl, text));

            ExcerptTailer tailer = q.createTailer();
            for (int i = 0; i < size; i++) {
                try (DocumentContext dc = tailer.readingDocument(false)) {
                    long index = dc.index();
                    long actual = dc.wire().read(() -> "key").int64();

                    Assert.assertEquals(toTextIndex(q, index), toTextIndex(q, actual));
                }
            }
        }

    }

    @NotNull
    private String toTextIndex(SingleChronicleQueue q, long index) {
        return Long.toHexString(q.rollCycle().toCycle(index)) + "_" + Long.toHexString(q.rollCycle().toSequenceNumber(index));
    }

    @Ignore("Long Running Test")
    @Test
    public void testRandomConcurrentReadWrite() throws
            InterruptedException {

        // some text to simulate load.
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 5; i++) sb.append(UUID.randomUUID());
        String text = sb.toString();

        for (int i = 0; i < 20; i++) {
            ExecutorService executor = Executors.newWorkStealingPool(8);
            try (ChronicleQueue q = builder(getTmpDir(), this.wireType)
                    .rollCycle(MINUTELY)
                    .build()) {

                final ThreadLocal<ExcerptAppender> tl = ThreadLocal.withInitial(() -> {
                    final ExcerptAppender appender = q.acquireAppender();
                    appender.padToCacheAlign(ALWAYS);
                    return appender;
                });
                final ThreadLocal<ExcerptTailer> tlt = ThreadLocal.withInitial(q::createTailer);

                int size = 20_000_000;

                for (int j = 0; j < size; j++) {
                    executor.execute(() -> doSomething(tl, tlt, text));
                }

                executor.shutdown();
                if (!executor.awaitTermination(10_000, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }

                System.out.println(". " + i);
                Jvm.pause(1000);
            }
        }
    }

    @Test
    public void testToEndPrevCycleEOF() {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        File dir = getTmpDir();
        try (ChronicleQueue q = builder(dir, wireType)
                .rollCycle(TEST_SECONDLY)
                .timeProvider(clock::get)
                .build()) {

            q.acquireAppender()
                    .writeText("first");
        }

        clock.addAndGet(1100);

        // this will write an EOF
        try (ChronicleQueue q = builder(dir, wireType)
                .rollCycle(TEST_SECONDLY)
                .timeProvider(clock::get)
                .build()) {

            ExcerptTailer tailer = q.createTailer();

            Assert.assertEquals("first", tailer.readText());
            Assert.assertEquals(null, tailer.readText());

        }

        try (ChronicleQueue q = builder(dir, wireType)
                .rollCycle(TEST_SECONDLY)
                .timeProvider(clock::get)
                .build()) {

            ExcerptTailer tailer = q.createTailer().toEnd();

            try (DocumentContext documentContext = tailer.readingDocument()) {
                Assert.assertFalse(documentContext.isPresent());
            }

            try (DocumentContext documentContext = tailer.readingDocument()) {
                Assert.assertFalse(documentContext.isPresent());
            }
        }

        clock.addAndGet(50L);

        try (ChronicleQueue q = builder(dir, wireType)
                .rollCycle(TEST_SECONDLY)
                .timeProvider(clock::get)
                .build()) {

            ExcerptTailer excerptTailerBeforeAppend = q.createTailer().toEnd();
            q.acquireAppender().writeText("more text");
            ExcerptTailer excerptTailerAfterAppend = q.createTailer().toEnd();
            q.acquireAppender().writeText("even more text");

            Assert.assertEquals("more text", excerptTailerBeforeAppend.readText());
            Assert.assertEquals("even more text", excerptTailerAfterAppend.readText());
            Assert.assertEquals("even more text", excerptTailerBeforeAppend.readText());
        }
    }

    @Test
    public void shouldNotGenerateGarbageReadingDocumentAfterEndOfFile() {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        File dir = getTmpDir();
        try (ChronicleQueue q = builder(dir, wireType)
                .rollCycle(TEST_SECONDLY)
                .timeProvider(clock::get)
                .build()) {

            q.acquireAppender()
                    .writeText("first");
        }

        clock.addAndGet(1100);

        final List<String> foo = new ArrayList<>();
        final List<byte[]> foo2 = new ArrayList<>();
        // this will write an EOF
        try (ChronicleQueue q = builder(dir, wireType)
                .rollCycle(TEST_SECONDLY)
                .timeProvider(clock::get)
                .build()) {

            ExcerptTailer tailer = q.createTailer();

            Assert.assertEquals("first", tailer.readText());
            GcControls.waitForGcCycle();
            final long startCollectionCount = GcControls.getGcCount();

            // allow a few GCs due to possible side-effect or re-used JVM
            final long maxAllowedGcCycles = 6;
            final long endCollectionCount = GcControls.getGcCount();
            final long actualGcCycles = endCollectionCount - startCollectionCount;

            assertTrue(String.format("Too many GC cycles. Expected <= %d, but was %d",
                    maxAllowedGcCycles, actualGcCycles),
                    actualGcCycles <= maxAllowedGcCycles);
        }
    }


    @Test
    public void testTailerWhenCyclesWhereSkippedOnWrite() {
        SetTimeProvider timeProvider = new SetTimeProvider();

        try (final RollingChronicleQueue queue = binary(getTmpDir())
                .rollCycle(RollCycles.TEST_SECONDLY).timeProvider(timeProvider)
                .build()) {
            final ExcerptAppender appender = queue.acquireAppender();
            final ExcerptTailer tailer = queue.createTailer();

            final List<String> stringsToPut = Arrays.asList("one", "two", "three");

            // writes two strings immediately and one string with 2 seconds delay
            {
                try (DocumentContext writingContext = appender.writingDocument()) {
                    writingContext.wire()
                            .write().bytes(stringsToPut.get(0).getBytes());
                }
                try (DocumentContext writingContext = appender.writingDocument()) {
                    writingContext.wire()
                            .write().bytes(stringsToPut.get(1).getBytes());
                }
                timeProvider.advanceMillis(2100);
                try (DocumentContext writingContext = appender.writingDocument()) {
                    writingContext.wire().write().bytes(stringsToPut.get(2).getBytes());
                }
            }

            System.out.println(queue.dump());

            for (String expected : stringsToPut) {
                try (DocumentContext readingContext = tailer.readingDocument()) {
                    if (!readingContext.isPresent())
                        Assert.fail();
                    String text = readingContext.wire().read().text();
                    System.out.println("read=" + text);
                    Assert.assertEquals(expected, text);

                }
            }
        }
    }

    private void doSomething(@NotNull ThreadLocal<ExcerptAppender> tla, @NotNull ThreadLocal<ExcerptTailer> tlt, String text) {
        if (Math.random() > 0.5)
            writeTestDocument(tla, text);
        else
            readDocument(tlt, text);
    }

    private void readDocument(@NotNull ThreadLocal<ExcerptTailer> tlt, String text) {
        try (DocumentContext dc = tlt.get().readingDocument()) {
            if (!dc.isPresent())
                return;
            Assert.assertEquals(dc.index(), dc.wire().read(() -> "key").int64());
            Assert.assertEquals(text, dc.wire().read(() -> "text").text());
        }
    }

    private void writeTestDocument(@NotNull ThreadLocal<ExcerptAppender> tl, String text) {
        try (DocumentContext dc = tl.get().writingDocument()) {
            long index = dc.index();
            dc.wire().write("key").int64(index);
            dc.wire().write("text").text(text);
        }
    }

    @Test
    public void testMultipleAppenders() {
        try (ChronicleQueue syncQ = builder(getTmpDir(), this.wireType)
                .rollCycle(TEST_DAILY)
                .build()) {

            ExcerptAppender syncA = syncQ.acquireAppender();
            ExcerptAppender syncB = syncQ.acquireAppender();
            ExcerptAppender syncC = syncQ.acquireAppender();
            int count = 0;
            for (int i = 0; i < 3; i++) {
                syncA.writeText("hello A" + i);
                assertEquals(count++, (int) syncA.lastIndexAppended());
                syncB.writeText("hello B" + i);
                assertEquals(count++, (int) syncB.lastIndexAppended());
                try (DocumentContext dc = syncC.writingDocument(true)) {
                    dc.wire().getValueOut().text("some meta " + i);
                }
            }
            String expected = expectedMultipleAppenders();
            assertEquals(expected, syncQ.dump());
        }
    }

    @NotNull
    protected String expectedMultipleAppenders() {

        if (wireType == WireType.BINARY)
            return "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: [\n" +
                    "    732,\n" +
                    "    3143916060677\n" +
                    "  ],\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 442,\n" +
                    "    lastIndex: 6\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  lastIndexReplicated: -1,\n" +
                    "  sourceId: 0\n" +
                    "}\n" +
                    "# position: 442, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  544,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 544, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 6\n" +
                    "  640,\n" +
                    "  652,\n" +
                    "  680,\n" +
                    "  692,\n" +
                    "  720,\n" +
                    "  732,\n" +
                    "  0, 0\n" +
                    "]\n" +
                    "# position: 640, header: 0\n" +
                    "--- !!data\n" +
                    "hello A0\n" +
                    "# position: 652, header: 1\n" +
                    "--- !!data\n" +
                    "hello B0\n" +
                    "# position: 664, header: 1\n" +
                    "--- !!meta-data #binary\n" +
                    "some meta 0\n" +
                    "# position: 680, header: 2\n" +
                    "--- !!data\n" +
                    "hello A1\n" +
                    "# position: 692, header: 3\n" +
                    "--- !!data\n" +
                    "hello B1\n" +
                    "# position: 704, header: 3\n" +
                    "--- !!meta-data #binary\n" +
                    "some meta 1\n" +
                    "# position: 720, header: 4\n" +
                    "--- !!data\n" +
                    "hello A2\n" +
                    "# position: 732, header: 5\n" +
                    "--- !!data\n" +
                    "hello B2\n" +
                    "# position: 744, header: 5\n" +
                    "--- !!meta-data #binary\n" +
                    "some meta 2\n" +
                    "...\n" +
                    "# 130308 bytes remaining\n";

        if (wireType == WireType.BINARY_LIGHT)
            return "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: [\n" +
                    "    732,\n" +
                    "    3143916060677\n" +
                    "  ],\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 442,\n" +
                    "    lastIndex: 6\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  lastIndexReplicated: -1,\n" +
                    "  sourceId: 0\n" +
                    "}\n" +
                    "# position: 442, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  544,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 544, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 6\n" +
                    "  640,\n" +
                    "  652,\n" +
                    "  680,\n" +
                    "  692,\n" +
                    "  720,\n" +
                    "  732,\n" +
                    "  0, 0\n" +
                    "]\n" +
                    "# position: 640, header: 0\n" +
                    "--- !!data\n" +
                    "hello A0\n" +
                    "# position: 652, header: 1\n" +
                    "--- !!data\n" +
                    "hello B0\n" +
                    "# position: 664, header: 1\n" +
                    "--- !!meta-data #binary\n" +
                    "some meta 0\n" +
                    "# position: 680, header: 2\n" +
                    "--- !!data\n" +
                    "hello A1\n" +
                    "# position: 692, header: 3\n" +
                    "--- !!data\n" +
                    "hello B1\n" +
                    "# position: 704, header: 3\n" +
                    "--- !!meta-data #binary\n" +
                    "some meta 1\n" +
                    "# position: 720, header: 4\n" +
                    "--- !!data\n" +
                    "hello A2\n" +
                    "# position: 732, header: 5\n" +
                    "--- !!data\n" +
                    "hello B2\n" +
                    "# position: 744, header: 5\n" +
                    "--- !!meta-data #binary\n" +
                    "some meta 2\n" +
                    "...\n" +
                    "# 130308 bytes remaining\n";
        throw new IllegalStateException("unknown wiretype=" + wireType);
    }

    @Test
    public void testCountExceptsBetweenCycles() {
        SetTimeProvider timeProvider = new SetTimeProvider();

        final SingleChronicleQueueBuilder builder = binary(getTmpDir())
                .rollCycle(RollCycles.TEST_SECONDLY).timeProvider(timeProvider);
        final RollingChronicleQueue queue = builder.build();
        final ExcerptAppender appender = queue.createAppender();

        long[] indexs = new long[10];
        for (int i = 0; i < indexs.length; i++) {
            System.out.println(".");
            try (DocumentContext writingContext = appender.writingDocument()) {
                writingContext.wire().write().text("some-text-" + i);
                indexs[i] = writingContext.index();
            }

            // we add the pause times to vary the test, to ensure it can handle when cycles are
            // skipped
            if ((i + 1) % 5 == 0)
                timeProvider.advanceMillis(2000);
            else if ((i + 1) % 3 == 0)
                timeProvider.advanceMillis(1000);
        }

        for (int lower = 0; lower < indexs.length; lower++) {
            for (int upper = lower; upper < indexs.length; upper++) {
                System.out.println("lower=" + lower + ",upper=" + upper);
                Assert.assertEquals(upper - lower, queue.countExcerpts(indexs[lower],
                        indexs[upper]));
            }
        }

        // check the base line of the test below
        Assert.assertEquals(6, queue.countExcerpts(indexs[0], indexs[6]));

        /// check for the case when the last index has a sequence number of -1
        Assert.assertEquals(queue.rollCycle().toSequenceNumber(indexs[6]), 0);
        Assert.assertEquals(5, queue.countExcerpts(indexs[0],
                indexs[6] - 1));

        /// check for the case when the first index has a sequence number of -1
        Assert.assertEquals(7, queue.countExcerpts(indexs[0] - 1,
                indexs[6]));

    }

    @Test
    public void testReadingWritingWhenNextCycleIsInSequence() {
        SetTimeProvider timeProvider = new SetTimeProvider();

        final File dir = DirectoryUtils.tempDir(testName.getMethodName());
        final RollCycles rollCycle = RollCycles.TEST_SECONDLY;

        // write first message
        try (ChronicleQueue queue = binary(dir)
                .rollCycle(rollCycle).timeProvider(timeProvider).build()) {
            queue.acquireAppender().writeText("first message");
        }

        timeProvider.advanceMillis(1100);

        // write second message
        try (ChronicleQueue queue = binary(dir)
                .rollCycle(rollCycle).timeProvider(timeProvider).build()) {
            queue.acquireAppender().writeText("second message");
        }

        // read both messages
        try (ChronicleQueue queue = binary(dir)
                .rollCycle(rollCycle).timeProvider(timeProvider).build()) {
            ExcerptTailer tailer = queue.createTailer();
            Assert.assertEquals("first message", tailer.readText());
            Assert.assertEquals("second message", tailer.readText());
        }
    }

    @Test
    public void testReadingWritingWhenCycleIsSkipped() {
        SetTimeProvider timeProvider = new SetTimeProvider();

        final File dir = DirectoryUtils.tempDir(testName.getMethodName());
        final RollCycles rollCycle = RollCycles.TEST_SECONDLY;

        // write first message
        try (ChronicleQueue queue = binary(dir)
                .rollCycle(rollCycle).timeProvider(timeProvider)
                .build()) {
            queue.acquireAppender().writeText("first message");
        }

        timeProvider.advanceMillis(2100);

        // write second message
        try (ChronicleQueue queue = binary(dir)
                .rollCycle(rollCycle).timeProvider(timeProvider).build()) {
            queue.acquireAppender().writeText("second message");
        }

        // read both messages
        try (ChronicleQueue queue = binary(dir)
                .rollCycle(rollCycle).timeProvider(timeProvider).build()) {
            ExcerptTailer tailer = queue.createTailer();
            Assert.assertEquals("first message", tailer.readText());
            Assert.assertEquals("second message", tailer.readText());
        }

    }

    @Test
    public void testReadingWritingWhenCycleIsSkippedBackwards() throws Exception {

        final File dir = DirectoryUtils.tempDir(testName.getMethodName());
        final RollCycles rollCycle = RollCycles.TEST_SECONDLY;

        // write first message
        try (ChronicleQueue queue =
                     binary(dir)
                             .rollCycle(rollCycle).build()) {
            queue.acquireAppender().writeText("first message");
        }

        //TODO: this test fails when converted to use a TimeProvider. Need to work out why
        Thread.sleep(2100);

        // write second message
        try (ChronicleQueue queue =
                     binary(dir)
                             .rollCycle(rollCycle).build()) {
            queue.acquireAppender().writeText("second message");
        }

        // read both messages
        try (ChronicleQueue queue =
                     binary(dir)
                             .rollCycle(rollCycle).build()) {
            ExcerptTailer tailer = queue.createTailer();
            ExcerptTailer excerptTailer = tailer.direction(TailerDirection.BACKWARD).toEnd();
            Assert.assertEquals("second message", excerptTailer.readText());
            Assert.assertEquals("first message", excerptTailer.readText());
        }
    }

    @Test
    public void testReadWritingWithTimeProvider() {
        final File dir = DirectoryUtils.tempDir(testName.getMethodName());

        long time = System.currentTimeMillis();

        try (ChronicleQueue q1 = binary(dir)
                .timeProvider(() -> time)
                .build()) {

            try (SingleChronicleQueue q2 = binary(dir)
                    .timeProvider(() -> time)
                    .build()) {

                final ExcerptAppender appender2 = q2.acquireAppender();
                final ExcerptTailer tailer1 = q1.createTailer();
                final ExcerptTailer tailer2 = q2.createTailer();

                try (final DocumentContext dc = appender2.writingDocument()) {
                    dc.wire().write().text("some data");
                }

                try (DocumentContext dc = tailer2.readingDocument()) {
                    Assert.assertTrue(dc.isPresent());
                }

                Assert.assertTrue(q1.file().equals(q2.file()));

                for (int i = 0; i < 10; i++) {
                    try (DocumentContext dc = tailer1.readingDocument()) {
                        if (dc.isPresent())
                            return;
                    }
                    Jvm.pause(1);
                }
                fail();
            }
        }
    }

    @Test
    public void testTailerSnappingRollWithNewAppender() throws Exception {
        SetTimeProvider timeProvider = new SetTimeProvider();
        final File dir = DirectoryUtils.tempDir(testName.getMethodName());
        final RollCycles rollCycle = RollCycles.TEST_SECONDLY;

        // write first message
        try (ChronicleQueue queue =
                     binary(dir)
                             .rollCycle(rollCycle).timeProvider(timeProvider).build()) {

            ExcerptAppender excerptAppender = queue.acquireAppender();
            excerptAppender.writeText("someText");

            ExecutorService executorService = Executors.newFixedThreadPool(2);

            Future f1 = executorService.submit(() -> {

                try (ChronicleQueue queue2 = binary(dir)
                        .rollCycle(rollCycle).timeProvider(timeProvider).build()) {
                    queue2.acquireAppender().writeText("someText more");
                }
                timeProvider.advanceMillis(1100);
                try (ChronicleQueue queue2 = binary(dir)
                        .rollCycle(rollCycle).build()) {
                    queue2.acquireAppender().writeText("someText more");
                }

            });

            Future f2 = executorService.submit(() -> {

                // write second message
                try (ChronicleQueue queue2 = binary(dir)
                        .rollCycle(rollCycle).timeProvider(timeProvider).build()) {

                    for (int i = 0; i < 5; i++) {
                        queue2.acquireAppender().writeText("someText more");
                        timeProvider.advanceMillis(400);
                    }
                }

            });

            f1.get();
            f2.get();

            executorService.shutdownNow();
        }
    }

    @Test
    public void testLongLivingTailerAppenderReAcquiredEachSecond() {
        SetTimeProvider timeProvider = new SetTimeProvider();
        final File dir = DirectoryUtils.tempDir(testName.getMethodName());
        final RollCycles rollCycle = RollCycles.TEST_SECONDLY;

        try (ChronicleQueue queuet = binary(dir)
                .rollCycle(rollCycle)
                .timeProvider(timeProvider)
                .build()) {

            final ExcerptTailer tailer = queuet.createTailer();

            // write first message
            try (ChronicleQueue queue =
                         binary(dir)
                                 .rollCycle(rollCycle)
                                 .timeProvider(timeProvider)
                                 .build()) {

                for (int i = 0; i < 5; i++) {

                    final ExcerptAppender appender = queue.acquireAppender();
                    timeProvider.advanceMillis(1100);
                    try (final DocumentContext dc = appender.writingDocument()) {
                        dc.wire().write("some").int32(i);
                    }

                    try (final DocumentContext dc = tailer.readingDocument()) {
                        Assert.assertEquals(i, dc.wire().read("some").int32());
                    }
                }
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testCountExceptsWithRubbishData() {

        try (final RollingChronicleQueue queue = binary(getTmpDir())
                .rollCycle(RollCycles.TEST_SECONDLY)
                .build()) {

            // rubbish data
            queue.countExcerpts(0x578F542D00000000L, 0x528F542D00000000L);
        }
    }

    //    @Ignore("todo fix fails with enterprise queue")
    @Test
    public void testFromSizePrefixedBlobs() {

        try (final RollingChronicleQueue queue = binary(getTmpDir())
                .build()) {

            try (DocumentContext dc = queue.acquireAppender().writingDocument()) {
                dc.wire().write("some").text("data");
            }
            String s = null;

            DocumentContext dc0;
            try (DocumentContext dc = queue.createTailer().readingDocument()) {
                s = Wires.fromSizePrefixedBlobs(dc);
                if (!encryption)
                    Assert.assertTrue(s.contains("some: data"));
                dc0 = dc;
            }

            String out = Wires.fromSizePrefixedBlobs(dc0);
            Assert.assertEquals(s, out);

        }

    }

    @Test
    public void tailerRollBackTest() {
        final File source = DirectoryUtils.tempDir("testCopyQueue-source");
        try (final RollingChronicleQueue q = binary(source).build()) {

            try (DocumentContext dc = q.acquireAppender().writingDocument()) {
                dc.wire().write("hello").text("hello-world");
            }

            try (DocumentContext dc = q.acquireAppender().writingDocument()) {
                dc.wire().write("hello2").text("hello-world-2");
            }

        }
    }


    @Test
    public void testCopyQueue() {
        final File source = DirectoryUtils.tempDir("testCopyQueue-source");
        final File target = DirectoryUtils.tempDir("testCopyQueue-target");
        {

            try (final RollingChronicleQueue q =
                         binary(source)
                                 .build()) {

                ExcerptAppender excerptAppender = q.acquireAppender();
                excerptAppender.writeMessage(() -> "one", 1);
                excerptAppender.writeMessage(() -> "two", 2);
                excerptAppender.writeMessage(() -> "three", 3);
                excerptAppender.writeMessage(() -> "four", 4);
            }
        }
        {
            try (final RollingChronicleQueue s =
                         binary(source)
                                 .build();
                 final RollingChronicleQueue t =
                         binary(target)
                                 .build()) {
                ExcerptTailer sourceTailer = s.createTailer();

                ExcerptAppender appender = t.acquireAppender();

                for (; ; ) {
                    try (DocumentContext rdc = sourceTailer.readingDocument()) {
                        if (!rdc.isPresent())
                            break;

                        try (DocumentContext wdc = appender.writingDocument(rdc.index())) {
                            final Bytes<?> bytes = rdc.wire().bytes();
                            System.out.println(bytes);
                            wdc.wire().bytes().write(bytes);
                        }

                    }
                }
            }
        }
    }

    /**
     * see https://github.com/OpenHFT/Chronicle-Queue/issues/299
     */
    @Test
    public void testIncorrectExcerptTailerReadsAfterSwitchingTailerDirection() {

        final RollingChronicleQueue queue = binary(getTmpDir())
                .rollCycle(RollCycles.TEST_SECONDLY).build();

        int value = 0;
        long cycle = 0;

        long startIndex = 0;
        for (int i = 0; i < 56; i++) {
            try (final DocumentContext dc = queue.acquireAppender().writingDocument()) {

                if (cycle == 0)
                    cycle = queue.rollCycle().toCycle(dc.index());
                final long index = dc.index();
                final long seq = queue.rollCycle().toSequenceNumber(index);

                if (seq == 52)
                    startIndex = dc.index();

                if (seq >= 52) {
                    final int v = value++;
                    System.out.println("stored => sequence-number=" + seq + ", value=" + v);
                    dc.wire().write("value").int64(v);
                } else {
                    dc.wire().write("value").int64(0);
                }

            }
        }

        ExcerptTailer tailer = queue.createTailer();

        Assert.assertTrue(tailer.moveToIndex(startIndex));

        tailer = tailer.direction(TailerDirection.FORWARD);
        Assert.assertEquals(0, action(tailer, queue.rollCycle()));
        Assert.assertEquals(1, action(tailer, queue.rollCycle()));

        tailer = tailer.direction(TailerDirection.BACKWARD);
        Assert.assertEquals(2, action(tailer, queue.rollCycle()));
        Assert.assertEquals(1, action(tailer, queue.rollCycle()));

        tailer = tailer.direction(TailerDirection.FORWARD);
        Assert.assertEquals(0, action(tailer, queue.rollCycle()));
        Assert.assertEquals(1, action(tailer, queue.rollCycle()));
    }

    private long action(@NotNull final ExcerptTailer tailer1, @NotNull final RollCycle rollCycle) {

        //  tailer1.direction(direction);
        long readvalue = 0;
        long seqNumRead = 0;
        try (final DocumentContext dc = tailer1.readingDocument()) {

            readvalue = dc.wire().read("value").int64();
            seqNumRead = dc.index();
        }

        final long nextSeq = rollCycle.toSequenceNumber(tailer1.index());
        System.out.println("Return-value=" + readvalue +
                ", seq=" + rollCycle.toSequenceNumber(seqNumRead) +
                ", next-seq=" + nextSeq + "(" + Long.toHexString(nextSeq) + "x0)" + ",direction="
                + tailer1.direction());

        return readvalue;
    }

    @Test
    public void checkReferenceCountingAndCheckFileDeletion() {

        MappedFile mappedFile;
        {
            try (RollingChronicleQueue queue =
                         binary(getTmpDir())
                                 .rollCycle(RollCycles.TEST_SECONDLY)
                                 .build()) {
                ExcerptAppender appender = queue.acquireAppender();

                try (DocumentContext documentContext1 = appender.writingDocument()) {
                    documentContext1.wire().write().text("some text");
                }

                try (DocumentContext documentContext = queue.createTailer().readingDocument()) {
                    mappedFile = toMappedFile(documentContext);
                    Assert.assertEquals("some text", documentContext.wire().read().text());
                }

            }

            waitFor(mappedFile::isClosed, "mappedFile is not closed");

            // this used to fail on windows
            Assert.assertTrue(mappedFile.file().delete());
        }

    }

    @Test
    public void checkReferenceCountingWhenRollingAndCheckFileDeletion() {
        SetTimeProvider timeProvider = new SetTimeProvider();

        MappedFile mappedFile1, mappedFile2, mappedFile3, mappedFile4;
        {
            try (RollingChronicleQueue queue =
                         binary(getTmpDir())
                                 .rollCycle(RollCycles.TEST_SECONDLY)
                                 .timeProvider(timeProvider)
                                 .build()) {
                ExcerptAppender appender = queue.acquireAppender();

                try (DocumentContext dc = appender.writingDocument()) {
                    dc.wire().write().text("some text");
                    mappedFile1 = toMappedFile(dc);
                }
                timeProvider.advanceMillis(1100);
                try (DocumentContext dc = appender.writingDocument()) {
                    dc.wire().write().text("some more text");
                    mappedFile2 = toMappedFile(dc);
                }

                ExcerptTailer tailer = queue.createTailer();
                try (DocumentContext documentContext = tailer.readingDocument()) {
                    mappedFile3 = toMappedFile(documentContext);
                    Assert.assertEquals("some text", documentContext.wire().read().text());

                }

                try (DocumentContext documentContext = tailer.readingDocument()) {
                    mappedFile4 = toMappedFile(documentContext);
                    Assert.assertEquals("some more text", documentContext.wire().read().text());

                }
            }

            waitFor(mappedFile1::isClosed, "mappedFile1 is not closed");
            waitFor(mappedFile2::isClosed, "mappedFile2 is not closed");

            // this used to fail on windows
            Assert.assertTrue(mappedFile1.file().delete());
            Assert.assertTrue(mappedFile2.file().delete());
        }
    }

    @Test
    public void testWritingDocumentIsAtomic() {

        final int threadCount = 8;
        final ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        // remove change of cycle roll in test, cross-cycle atomicity is covered elsewhere
        final AtomicLong fixedClock = new AtomicLong(System.currentTimeMillis());
        try {
            SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(getTmpDir()).
                    rollCycle(RollCycles.TEST_SECONDLY).
                    timeoutMS(3_000).timeProvider(fixedClock::get).
                    testBlockSize().build();
            final int iterationsPerThread = Short.MAX_VALUE / 8;
            final int totalIterations = iterationsPerThread * threadCount;
            final int[] nonAtomicCounter = new int[]{0};
            for (int i = 0; i < threadCount; i++) {
                executorService.submit(() -> {
                    for (int j = 0; j < iterationsPerThread; j++) {
                        ExcerptAppender excerptAppender = queue.acquireAppender();

                        try (DocumentContext dc = excerptAppender.writingDocument()) {
                            int value = nonAtomicCounter[0]++;
                            dc.wire().write("some key").int64(value);
                        }
                    }
                });
            }

            long timeout = 20_000 + System.currentTimeMillis();
            ExcerptTailer tailer = queue.createTailer();
            for (int expected = 0; expected < totalIterations; expected++) {
                for (; ; ) {
                    if (System.currentTimeMillis() > timeout)
                        Assert.fail("Timed out, having read " + expected + " documents");
                    try (DocumentContext dc = tailer.readingDocument()) {
                        if (!dc.isPresent()) {
                            Thread.yield();
                            continue;
                        }

                        long justRead = dc.wire().read("some key").int64();
                        Assert.assertEquals(expected, justRead);
                        break;
                    }
                }
            }
        } finally {
            executorService.shutdownNow();

            try {
                executorService.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                executorService.shutdownNow();
            }
        }
    }

    @Test
    public void shouldBeAbleToLoadQueueFromReadOnlyFiles() throws Exception {
        final File queueDir = getTmpDir();
        try (final SingleChronicleQueue queue = builder(queueDir, wireType).
                testBlockSize().build()) {
            queue.acquireAppender().writeDocument("foo", (v, t) -> {
                v.text(t);
            });
        }

        Files.list(queueDir.toPath()).forEach(p -> {
            assertTrue(p.toFile().setWritable(false));
        });

        try (final SingleChronicleQueue queue = builder(queueDir, wireType).
                readOnly(true).
                testBlockSize().build()) {
            assertTrue(queue.createTailer().readingDocument().isPresent());
        }
    }

    interface Msg {
        void msg(String s);
    }

    @Test
    public void shouldCreateQueueInCurrentDirectory() {
        try (final SingleChronicleQueue queue =
                     builder(new File(""), wireType).
                             testBlockSize().build()) {

        }

        assertThat(new File("directory-listing.cq4t").delete(), is(true));
    }

    @NotNull
    protected SingleChronicleQueueBuilder builder(@NotNull File file, @NotNull WireType wireType) {
        return SingleChronicleQueueBuilder.builder(file, wireType).rollCycle(RollCycles.TEST4_DAILY).testBlockSize();
    }

    @NotNull
    protected SingleChronicleQueueBuilder binary(@NotNull File file) {
        return builder(file, WireType.BINARY_LIGHT);
    }

    private MappedFile toMappedFile(@NotNull DocumentContext documentContext) {
        MappedFile mappedFile;
        MappedBytes bytes = (MappedBytes) documentContext.wire().bytes();
        mappedFile = bytes.mappedFile();
        return mappedFile;
    }

    @After
    public void checkMappedFiles() {
        System.gc();
        Jvm.pause(50);

        MappedFile.checkMappedFiles();
    }

    private static class MapWrapper extends AbstractMarshallable {
        final Map<CharSequence, Double> map = new HashMap<>();
    }

    static class MyMarshable extends AbstractMarshallable implements Demarshallable {
        @UsedViaReflection
        String name;

        @UsedViaReflection
        public MyMarshable(@NotNull WireIn wire) {
            readMarshallable(wire);
        }

        public MyMarshable() {
        }

    }

    private static long countEntries(final ChronicleQueue queue) {
        final ExcerptTailer tailer = queue.createTailer();
        tailer.toStart().direction(TailerDirection.FORWARD);
        long entryCount = 0L;
        while (true) {
            try (final DocumentContext ctx = tailer.readingDocument()) {
                if (!ctx.isPresent()) {
                    break;
                }

                entryCount++;
            }
        }

        return entryCount;
    }

    private static void waitFor(final Supplier<Boolean> condition, final String message) {
        final long timeoutAt = System.currentTimeMillis() + 10_000L;
        while (System.currentTimeMillis() < timeoutAt) {
            if (condition.get()) {
                return;
            }
        }

        fail(message);
    }

    @Test
    public void writeBytesAndIndexFiveTimesWithOverwriteTest() {


        try (final SingleChronicleQueue sourceQueue =
                     builder(DirectoryUtils.tempDir("to-be-deleted"), wireType).
                             testBlockSize().build()) {

            for (int i = 0; i < 5; i++) {
                ExcerptAppender excerptAppender = sourceQueue.acquireAppender();
                try (DocumentContext dc = excerptAppender.writingDocument()) {
                    dc.wire().write("hello").text("world" + i);
                }
            }

            ExcerptTailer tailer = sourceQueue.createTailer();

            try (final SingleChronicleQueue queue =
                         builder(DirectoryUtils.tempDir("to-be-deleted"), wireType).testBlockSize().build()) {


                ExcerptAppender appender0 = queue.acquireAppender();

                if (!(appender0 instanceof InternalAppender))
                    return;
                InternalAppender appender = (InternalAppender) appender0;

                if (!(appender instanceof StoreAppender))
                    return;
                List<BytesWithIndex> bytesWithIndies = new ArrayList<>();
                try {
                    for (int i = 0; i < 5; i++) {
                        bytesWithIndies.add(bytes(tailer));
                    }

                    for (int i = 0; i < 4; i++) {
                        BytesWithIndex b = bytesWithIndies.get(i);
                        appender.writeBytes(b.index, b.bytes);
                    }

                    for (int i = 0; i < 4; i++) {
                        BytesWithIndex b = bytesWithIndies.get(i);
                        appender.writeBytes(b.index, b.bytes);
                    }

                    BytesWithIndex b = bytesWithIndies.get(4);
                    appender.writeBytes(b.index, b.bytes);


                    ((StoreAppender) appender).checkWritePositionHeaderNumber();
                    appender0.writeText("hello");
                } finally {
                    closeQuietly(bytesWithIndies);
                }

                System.out.println(queue.dump());
                Assert.assertTrue(queue.dump().contains("--- !!data #binary\n" +
                        "hello: world0\n" +
                        "# position: 1041, header: 1\n" +
                        "--- !!data #binary\n" +
                        "hello: world1\n" +
                        "# position: 1058, header: 2\n" +
                        "--- !!data #binary\n" +
                        "hello: world2\n" +
                        "# position: 1075, header: 3\n" +
                        "--- !!data #binary\n" +
                        "hello: world3\n" +
                        "# position: 1092, header: 4\n" +
                        "--- !!data #binary\n" +
                        "hello: world4\n" +
                        "# position: 1109, header: 5\n" +
                        "--- !!data\n" +
                        "hello\n"));

            }

        }
    }


    @Test
    public void writeBytesAndIndexFiveTimesTest() throws IOException {


        try (final SingleChronicleQueue sourceQueue =
                     builder(DirectoryUtils.tempDir("to-be-deleted"), wireType).
                             testBlockSize().build()) {

            for (int i = 0; i < 5; i++) {
                ExcerptAppender excerptAppender = sourceQueue.acquireAppender();
                try (DocumentContext dc = excerptAppender.writingDocument()) {
                    dc.wire().write("hello").text("world" + i);
                }
            }

            ExcerptTailer tailer = sourceQueue.createTailer();

            try (final SingleChronicleQueue queue =
                         builder(DirectoryUtils.tempDir("to-be-deleted"), wireType).testBlockSize().build()) {


                ExcerptAppender appender = queue.acquireAppender();

                if (!(appender instanceof StoreAppender))
                    return;

                for (int i = 0; i < 5; i++) {
                    try (final BytesWithIndex b = bytes(tailer)) {
                        ((InternalAppender) appender).writeBytes(b.index, b.bytes);
                    }
                }

                String dump = queue.dump();
                System.out.println(dump);
                Assert.assertTrue(dump.contains( "# position: 1024, header: 0\n" +
                        "--- !!data #binary\n" +
                        "hello: world0\n" +
                        "# position: 1041, header: 1\n" +
                        "--- !!data #binary\n" +
                        "hello: world1\n" +
                        "# position: 1058, header: 2\n" +
                        "--- !!data #binary\n" +
                        "hello: world2\n" +
                        "# position: 1075, header: 3\n" +
                        "--- !!data #binary\n" +
                        "hello: world3\n" +
                        "# position: 1092, header: 4\n" +
                        "--- !!data #binary\n" +
                        "hello: world4"));
            }

        }
    }

    private BytesWithIndex bytes(final ExcerptTailer tailer) {
        try (DocumentContext dc = tailer.readingDocument()) {

            if (!dc.isPresent())
                return null;

            Bytes<?> bytes = dc.wire().bytes();
            long index = dc.index();
            return new BytesWithIndex(bytes, index);
        }
    }

    private static class BytesWithIndex implements Closeable {
        private BytesStore bytes;
        private long index;

        public BytesWithIndex(Bytes<?> bytes, long index) {
            this.bytes = Bytes.allocateElasticDirect(bytes.readRemaining()).write(bytes);
            this.index = index;
        }

        @Override
        public void close() {
            bytes.release();
        }
    }

    @NotNull
    private static Object[] testConfiguration(final WireType binary, final boolean encrypted) {
        return new Object[]{binary.name() + " - " + (encrypted ? "" : "not ") + "encrypted" , binary, encrypted};
    }
}