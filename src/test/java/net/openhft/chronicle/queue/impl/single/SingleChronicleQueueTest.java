/*
 * Copyright 2016-2020 chronicle.software
 *
 * https://chronicle.software
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

import net.openhft.chronicle.bytes.*;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.core.util.StringUtils;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.testframework.FlakyTestRunner;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.threads.TimeoutPauser;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.*;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static net.openhft.chronicle.core.io.Closeable.closeQuietly;
import static net.openhft.chronicle.queue.RollCycles.*;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueue.QUEUE_METADATA_FILE;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueue.SUFFIX;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
public class SingleChronicleQueueTest extends ChronicleQueueTestBase {

    private static final long TIMES = (4L << 20L);
    @NotNull
    protected final WireType wireType;
    protected final boolean named;
    protected final Bytes<?> appenderListenerDump = Bytes.allocateElasticOnHeap(256);

    public SingleChronicleQueueTest(@NotNull WireType wireType, boolean named) {
        this.wireType = wireType;
        this.named = named;
    }

    @Parameters(name = "wireType={0}, named={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{WireType.BINARY_LIGHT, true},
                new Object[]{WireType.BINARY, false},
                new Object[]{WireType.BINARY_LIGHT, false}
        );
    }

    private static List<String> getMappedQueueFileCount() throws IOException, InterruptedException {

        final int processId = OS.getProcessId();
        final List<String> fileList = new ArrayList<>();

        final Process pmap = new ProcessBuilder("pmap", Integer.toString(processId)).start();
        pmap.waitFor();
        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(pmap.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.contains(SUFFIX)) {
                    fileList.add(line);
                }
            }
        }

        return fileList;
    }

    private static long countEntries(final ChronicleQueue queue, boolean named) {
        final ExcerptTailer tailer = queue.createTailer(named ? "named" : null);
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
    public void testAppend() {
        try (final ChronicleQueue queue =
                     builderWithAppendListener(getTmpDir(), wireType)
                             .build()) {

            final ExcerptAppender appender = queue.acquireAppender();
            for (int i = 0; i < 10; i++) {
                final int n = i;
                appender.writeDocument(w -> w.write(TestKey.test).int32(n));
                assertEquals(n, queue.rollCycle().toSequenceNumber(appender.lastIndexAppended()));
            }

            assertEquals(10L, countEntries(queue, named));
        }
        assertEquals(expectedForTestAppend(),
                appenderListenerDump.toString());
    }

    @NotNull
    protected String expectedForTestAppend() {
        return "" +
                "idx: 4a0400000000\n" +
                "# position: 784, header: 0\n" +
                "--- !!data #binary\n" +
                "test: 0\n" +
                "\n" +
                "idx: 4a0400000001\n" +
                "# position: 796, header: 0\n" +
                "--- !!data #binary\n" +
                "test: 1\n" +
                "\n" +
                "idx: 4a0400000002\n" +
                "# position: 808, header: 0\n" +
                "--- !!data #binary\n" +
                "test: 2\n" +
                "\n" +
                "idx: 4a0400000003\n" +
                "# position: 820, header: 0\n" +
                "--- !!data #binary\n" +
                "test: 3\n" +
                "\n" +
                "idx: 4a0400000004\n" +
                "# position: 832, header: 0\n" +
                "--- !!data #binary\n" +
                "test: 4\n" +
                "\n" +
                "idx: 4a0400000005\n" +
                "# position: 844, header: 0\n" +
                "--- !!data #binary\n" +
                "test: 5\n" +
                "\n" +
                "idx: 4a0400000006\n" +
                "# position: 856, header: 0\n" +
                "--- !!data #binary\n" +
                "test: 6\n" +
                "\n" +
                "idx: 4a0400000007\n" +
                "# position: 868, header: 0\n" +
                "--- !!data #binary\n" +
                "test: 7\n" +
                "\n" +
                "idx: 4a0400000008\n" +
                "# position: 880, header: 0\n" +
                "--- !!data #binary\n" +
                "test: 8\n" +
                "\n" +
                "idx: 4a0400000009\n" +
                "# position: 892, header: 0\n" +
                "--- !!data #binary\n" +
                "test: 9\n" +
                "\n";
    }

    @Test
    public void testTextReadWrite() {
        File tmpDir = getTmpDir();
        try (final ChronicleQueue queue =
                     builderWithAppendListener(tmpDir, wireType)
                             .build()) {
            queue.acquireAppender().writeText("hello world");
            assertEquals("hello world", queue.createTailer(named ? "named" : null).readText());
        }
        assertEquals(expectedForTestTextReadWrite(), appenderListenerDump.toString());
    }

    @NotNull
    protected String expectedForTestTextReadWrite() {
        return "" +
                "idx: 4a0400000000\n" +
                "# position: 784, header: 0\n" +
                "--- !!data #binary\n" +
                "hello world\n" +
                "\n";
    }

    @Test
    public void testCleanupDir() throws Throwable {
        if (OS.isWindows())
            FlakyTestRunner.builder(this::testCleanupDir0).build().run();
        else
            testCleanupDir0();
    }

    private void testCleanupDir0() {
        File tmpDir = getTmpDir();
        try (final ChronicleQueue queue =
                     builder(tmpDir, wireType)
                             .build()) {
            final ExcerptAppender appender = queue.acquireAppender();

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write("hello").text("world");
            }
        }
        // explicitly call so as to clean and release everything
        afterChecks();
        IOTools.deleteDirWithFilesOrThrow(tmpDir);
    }

    @Test
    public void testRollbackOnAppend() {
        try (final ChronicleQueue queue =
                     builder(getTmpDir(), wireType)
                             .build()) {

            final ExcerptAppender appender = queue.acquireAppender();

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write("hello").text("world");
            }

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write("hello").text("world2");
            }

            ExcerptTailer tailer = queue.createTailer(named ? "named" : null);

            try (DocumentContext dc = tailer.readingDocument()) {
                dc.wire().read("hello");
                dc.rollbackOnClose();
            }

            try (DocumentContext dc = tailer.readingDocument()) {
                assertEquals("world", dc.wire().read("hello").text());
            }

            try (DocumentContext dc = tailer.readingDocument()) {
                assertEquals("world2", dc.wire().read("hello").text());
            }
        }
    }

    @Test
    public void testWriteWithDocumentReadBytesDifferentThreads() throws InterruptedException, TimeoutException, ExecutionException {
        try (final ChronicleQueue queue = builder(getTmpDir(), wireType)
                .build()) {

            final String expected = "some long message";

            ExecutorService service1 = Executors.newSingleThreadExecutor(
                    new NamedThreadFactory("service1"));
            ScheduledExecutorService service2 = null;
            try {
                Future f = service1.submit(() -> {
                    try (final ExcerptAppender appender = queue.acquireAppender()) {

                        try (final DocumentContext dc = appender.writingDocument()) {
                            dc.wire().writeEventName("key").text(expected);
                        }
                    }
                });

                BlockingQueue<Bytes<?>> result = new ArrayBlockingQueue<>(10);

                service2 = Executors.newSingleThreadScheduledExecutor(
                        new NamedThreadFactory("service2"));
                service2.scheduleAtFixedRate(() -> {
                    Bytes<?> b = Bytes.allocateElasticOnHeap(128);
                    final ExcerptTailer tailer = queue.createTailer(named ? "named" : null);
                    tailer.readBytes(b);
                    if (b.readRemaining() == 0)
                        return;
                    b.readPosition(0);
                    result.add(b);
                    throw new RejectedExecutionException();
                }, 1, 1, TimeUnit.MICROSECONDS);

                final Bytes<?> bytes = result.poll(5, TimeUnit.SECONDS);
                if (bytes == null) {
                    // troubleshoot failed test http://teamcity.chronicle.software:8111/viewLog.html?buildId=264141&tab=buildResultsDiv&buildTypeId=OpenHFT_ChronicleQueue4_Snapshothttp://teamcity.chronicle.software:8111/viewLog.html?buildId=264141&tab=buildResultsDiv&buildTypeId=OpenHFT_ChronicleQueue4_Snapshothttp://teamcity.chronicle.software:8111/viewLog.html?buildId=264141&tab=buildResultsDiv&buildTypeId=OpenHFT_ChronicleQueue4_Snapshothttp://teamcity.chronicle.software:8111/viewLog.html?buildId=264141&tab=buildResultsDiv&buildTypeId=OpenHFT_ChronicleQueue4_Snapshothttp://teamcity.chronicle.software:8111/viewLog.html?buildId=264141&tab=buildResultsDiv&buildTypeId=OpenHFT_ChronicleQueue4_Snapshothttp://teamcity.chronicle.software:8111/viewLog.html?buildId=264141&tab=buildResultsDiv&buildTypeId=OpenHFT_ChronicleQueue4_Snapshothttp://teamcity.chronicle.software:8111/viewLog.html?buildId=264141&tab=buildResultsDiv&buildTypeId=OpenHFT_ChronicleQueue4_Snapshothttp://teamcity.chronicle.software:8111/viewLog.html?buildId=264141&tab=buildResultsDiv&buildTypeId=OpenHFT_ChronicleQueue4_Snapshothttp://teamcity.chronicle.software:8111/viewLog.html?buildId=264141&tab=buildResultsDiv&buildTypeId=OpenHFT_ChronicleQueue4_Snapshothttp://teamcity.chronicle.software:8111/viewLog.html?buildId=264141&tab=buildResultsDiv&buildTypeId=OpenHFT_ChronicleQueue4_Snapshothttp://teamcity.chronicle.software:8111/viewLog.html?buildId=264141&tab=buildResultsDiv&buildTypeId=OpenHFT_ChronicleQueue4_Snapshothttp://teamcity.chronicle.software:8111/viewLog.html?buildId=264141&tab=buildResultsDiv&buildTypeId=OpenHFT_ChronicleQueue4_Snapshot
                    f.get(1, TimeUnit.SECONDS);
                    throw new NullPointerException("nothing in result");
                }
                try {
                    final String actual = this.wireType.apply(bytes).read("key").text();
                    assertEquals(expected, actual);
                    f.get(1, TimeUnit.SECONDS);
                } finally {
                    bytes.releaseLast();
                }
            } finally {
                service1.shutdownNow();
                if (service2 != null)
                    service2.shutdownNow();
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void shouldBlowUpIfTryingToCreateQueueWithUnparseableRollCycle() {
        expectException("Overriding roll length from existing metadata");

        File tmpDir = getTmpDir();
        try (final ChronicleQueue queue = builder(tmpDir, wireType).rollCycle(new RollCycleDefaultingTest.MyRollcycle()).build()) {
            try (DocumentContext documentContext = queue.acquireAppender().writingDocument()) {
                documentContext.wire().write("somekey").text("somevalue");
            }
        }

        try (final ChronicleQueue ignored = builder(tmpDir, wireType).rollCycle(HOURLY).build()) {
        }
    }

    @Test
    public void testCanAppendMetadataIfAppendLockIsSet() {
        File tmpDir = getTmpDir();
        try (final ChronicleQueue queue = builder(tmpDir, wireType).build()) {
            ((SingleChronicleQueue) queue).appendLock().lock();
            final ExcerptAppender appender = queue.acquireAppender();
            Assume.assumeTrue("Failing in CQE", queue.acquireAppender() instanceof StoreAppender);
            try (DocumentContext dc = appender.writingDocument(true)) {
                dc.wire().write("Hello World");
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testCantAppendIfAppendLockIsSet() {
        File tmpDir = getTmpDir();
        try (final ChronicleQueue queue = builder(tmpDir, wireType).build()) {
            ((SingleChronicleQueue) queue).appendLock().lock();
            final ExcerptAppender appender = queue.acquireAppender();
            appender.writeText("Hello World");
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testCantAppendIfAppendLockIsSetInDifferentQueue() {
        expectException("Overriding roll length from existing metadata");
        expectException("Overriding roll cycle from");

        File tmpDir = getTmpDir();
        try (final ChronicleQueue queue = builder(tmpDir, wireType).build()) {
            ((SingleChronicleQueue) queue).appendLock().lock();
        }

        try (final ChronicleQueue queue = builder(tmpDir, wireType).rollCycle(new RollCycleDefaultingTest.MyRollcycle()).build()) {
            queue.acquireAppender().writeText("hello");
        }
    }

    @Test
    public void testCanAppendWriteBytesInternalIfAppendLockIsSet() {
        @NotNull Bytes<byte[]> test = Bytes.from("hello world");
        File tmpDir = getTmpDir();
        try (final ChronicleQueue queue = builderWithAppendListener(tmpDir, wireType).build()) {
            ((SingleChronicleQueue) queue).appendLock().lock();
            Assume.assumeTrue(queue.acquireAppender() instanceof StoreAppender);
            @NotNull ExcerptAppender appender = queue.acquireAppender();
            assumeTrue(appender instanceof StoreAppender);
            StoreAppender storeAppender = (StoreAppender) appender;
            ((SingleChronicleQueue) queue).writeLock().lock();
            storeAppender.writeBytesInternal(0, test);
        }
        assertEquals(expectedForTestCanAppendWriteBytesInternalIfAppendLockIsSet(), appenderListenerDump.toString());
    }

    @NotNull
    protected String expectedForTestCanAppendWriteBytesInternalIfAppendLockIsSet() {
        return "" +
                "idx: 0\n" +
                "# position: 784, header: 0\n" +
                "--- !!data\n" +
                "hello world\n" +
                "\n";
    }

    @Test
    public void shouldNotBlowUpIfTryingToCreateQueueWithIncorrectRollCycle() {
        expectException("Overriding roll length from existing metadata");
        expectException("Overriding roll cycle from");
        File tmpDir = getTmpDir();
        try (final ChronicleQueue queue = builder(tmpDir, wireType).rollCycle(DEFAULT).build()) {
            try (DocumentContext documentContext = queue.acquireAppender().writingDocument()) {
                documentContext.wire().write("somekey").text("somevalue");
            }
        }

        // we don't store which RollCycles enum was used and we try and match by format string, we
        // match the first RollCycles with the same format string, which may not
        // be the RollCycles it was written with
        try (final ChronicleQueue ignored = builder(tmpDir, wireType).rollCycle(HOURLY).build()) {
            assertEquals(DEFAULT, ignored.rollCycle());
        }
    }

    @Test
    public void shouldOverrideDifferentEpoch() {
        expectException("Overriding roll epoch from existing metadata, was 10, overriding to 100");
        File tmpDir = getTmpDir();
        try (final ChronicleQueue queue = builder(tmpDir, wireType).rollCycle(TEST_SECONDLY).epoch(100).build()) {
            try (DocumentContext documentContext = queue.acquireAppender().writingDocument()) {
                documentContext.wire().write("somekey").text("somevalue");
            }
        }

        try (final ChronicleQueue ignored = builder(tmpDir, wireType).rollCycle(TEST_SECONDLY).epoch(10).build()) {
            assertEquals(100, ((SingleChronicleQueue) ignored).epoch());
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

            try (DocumentContext documentContext2 = qTailer.createTailer(named ? "named" : null).readingDocument()) {
                String str = documentContext2.wire().read("somekey").text();
                assertEquals("somevalue", str);
            }
        }
    }

    private long toSeq(final ChronicleQueue q, final long index) {
        return q.rollCycle().toSequenceNumber(index);
    }

    @Ignore("TODO FIX, appears to have side effects so can't be run without forking")
    @Test
    public void testLastWritten() throws InterruptedException {
        // TODO FIX
        // AbstractCloseable.disableCloseableTracing();

        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(
                new NamedThreadFactory("test"));

        File outDir = getTmpDir();
        File inDir = getTmpDir();
        try {
            final SetTimeProvider tp = new SetTimeProvider();
            try (ChronicleQueue outQueue = builder(outDir, wireType).rollCycle(RollCycles.TEST_SECONDLY).sourceId(1).timeProvider(tp).build()) {
                try (ChronicleQueue inQueue = builder(inDir, wireType).rollCycle(RollCycles.TEST_SECONDLY).sourceId(2).timeProvider(tp).build()) {

                    // write some initial data to the inqueue
                    final SCQMsg msg = inQueue.acquireAppender().methodWriterBuilder(SCQMsg.class).get();

                    msg.msg("somedata-0");
                    assertEquals(1, inDir.listFiles(file -> file.getName().endsWith("cq4")).length);

                    tp.advanceMillis(1000);

                    // write data into the inQueue
                    msg.msg("somedata-1");
                    assertEquals(2, inDir.listFiles(file -> file.getName().endsWith("cq4")).length);

                    // read a message on the in queue and write it to the out queue
                    {
                        SCQMsg out = outQueue.acquireAppender().methodWriterBuilder(SCQMsg.class).get();
                        MethodReader methodReader = inQueue.createTailer(named ? "named" : null).methodReader((SCQMsg) out::msg);

                        // reads the somedata-0
                        methodReader.readOne();

                        // reads the somedata-1
                        methodReader.readOne();

                        assertFalse(methodReader.readOne());

                        tp.advanceMillis(1000);
                        assertFalse(methodReader.readOne());
                    }

                    assertEquals("trying to read should not create a file", 2, inDir.listFiles(file -> file.getName().endsWith("cq4")).length);

                    // write data into the inQueue
                    msg.msg("somedata-2");
                    assertEquals(3, inDir.listFiles(file -> file.getName().endsWith("cq4")).length);

                    // advance 2 cycles - we will end up with a missing file
                    tp.advanceMillis(2000);

                    msg.msg("somedata-3");
                    msg.msg("somedata-4");
                    assertEquals("Should be a missing cycle file", 4, inDir.listFiles(file -> file.getName().endsWith("cq4")).length);

                    AtomicReference<String> actualValue = new AtomicReference<>();

                    // check that we are able to pick up from where we left off, in other words the next read should be somedata-2
                    {
                        ExcerptTailer excerptTailer = inQueue.createTailer(named ? "named" : null).afterLastWritten(outQueue);
                        MethodReader methodReader = excerptTailer.methodReader((SCQMsg) actualValue::set);

                        methodReader.readOne();
                        assertEquals("somedata-2", actualValue.get());

                        methodReader.readOne();
                        assertEquals("somedata-3", actualValue.get());

                        methodReader.readOne();
                        assertEquals("somedata-4", actualValue.get());

                        assertFalse(methodReader.readOne());
                    }
                }
            }
        } finally {
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.SECONDS);
            executorService.shutdownNow();
            IOTools.deleteDirWithFiles(inDir);
            IOTools.deleteDirWithFiles(outDir);
        }
    }

    @Test
    public void shouldAllowDirectoryToBeDeletedWhenQueueIsClosed() throws IOException {
        if (OS.isWindows()) {
            System.err.println("#460 Cannot test deleting after close on windows");
            return;
        }

        final File dir = getTmpDir();
        try (final ChronicleQueue queue =
                     builder(dir, wireType).
                             testBlockSize().build()) {
            try (final DocumentContext dc = queue.acquireAppender().writingDocument()) {
                dc.wire().write().text("foo");
            }
            try (final DocumentContext dc = queue.createTailer(named ? "named" : null).readingDocument()) {
                assertEquals("foo", dc.wire().read().text());
            }
        }

        try (Stream<Path> paths = Files.walk(dir.toPath())) {
            final List<Path> unDeletable = paths
                    .filter(p -> !Files.isDirectory(p))
                    .filter(p -> !p.toFile().delete())
                    .collect(Collectors.toList());
            assertTrue("Unable to delete " + unDeletable, unDeletable.isEmpty());
        }
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

            final ExcerptTailer tailer = queue.createTailer(named ? "named" : null);

            // Sequential read
            for (int i = 0; i < 10; i++) {

                Bytes<?> b = Bytes.allocateDirect(8);

                tailer.readBytes(b);

                assertEquals(expected.readInt(0), b.readInt(0));

                b.releaseLast();
            }
        }
    }

    @Test
    public void testAppendAndRead() {
        try (final ChronicleQueue queue = builder(getTmpDir(), this.wireType)
                .build()) {

            final ExcerptAppender appender = queue.acquireAppender();
            final int cycle = appender.cycle();
            for (int i = 0; i < 10; i++) {
                final int n = i;
                appender.writeDocument(w -> w.write(TestKey.test).int32(n));
                assertEquals(n, queue.rollCycle().toSequenceNumber(appender.lastIndexAppended()));
            }

            final ExcerptTailer tailer = queue.createTailer(named ? "named" : null);

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
    public void testReadAndAppend() throws InterruptedException {
        try (final ChronicleQueue queue = builder(getTmpDir(), this.wireType).build()) {

            final CountDownLatch started = new CountDownLatch(1);
            int[] results = new int[2];

            Thread t = new Thread(() -> {
                try {
                    started.countDown();
                    final ExcerptTailer tailer = queue.createTailer(named ? "named" : null);
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
                    fail("exception");
                }
            });
            t.setDaemon(true);
            t.start();

            assertTrue(started.await(1, TimeUnit.SECONDS));

            final ExcerptAppender appender = queue.acquireAppender();
            for (int i = 0; i < 2; i++) {
                final int n = i;
                appender.writeDocument(w -> w.write(TestKey.test).int32(n));
            }

            t.join(1_000);

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
            ExcerptTailer tailer = queue.createTailer(named ? "named" : null);
            int cycle = appender.cycle();
            for (int i = 0; i <= 5; i++) {
                final int n = i;

                writeTo.accept(appender, n);
                assertEquals(cycle + i, appender.cycle());

                try (DocumentContext dc = tailer.readingDocument()) {
                    long index = tailer.index();
                    assertEquals(appender.cycle(), tailer.cycle());
                    assertEquals(cycle + i, DEFAULT.toCycle(index));
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
            assumeFalse(wireType == WireType.DEFAULT_ZERO_BINARY);
            final ExcerptTailer tailer = queue.createTailer(named ? "named" : null).toStart();
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
    public void testAppendAndReadAtIndex() {
        try (final ChronicleQueue queue = builder(getTmpDir(), this.wireType)
                .rollCycle(TEST2_DAILY)
                .build()) {

            final ExcerptAppender appender = queue.acquireAppender();
            appender.cycle();
            for (int i = 0; i < 5; i++) {
                final int n = i;
                appender.writeDocument(w -> w.write(TestKey.test).int32(n));
                assertEquals(i, queue.rollCycle().toSequenceNumber(appender.lastIndexAppended()));
            }

            final ExcerptTailer tailer = queue.createTailer(named ? "named" : null);
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
            appender.writeDocument(wire -> wire.write("FirstName").text("Steve"));
            appender.writeDocument(wire -> wire.write("Surname").text("Jobs"));

            StringBuilder first = new StringBuilder();
            StringBuilder surname = new StringBuilder();

            final ExcerptTailer tailer = chronicle.createTailer(named ? "named" : null);

            tailer.readDocument(wire -> wire.read("FirstName").text(first));
            tailer.readDocument(wire -> wire.read("Surname").text(surname));
            assertEquals("Steve Jobs", first + " " + surname);
        }
    }

    @Test
    public void testIndexWritingDocument() {
        try (final ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .build()) {

            final ExcerptAppender appender = chronicle.acquireAppender();

            long index;
            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write("FirstName").text("Quartilla");
                index = dc.index();
            }

            try (DocumentContext dc = appender.writingDocument(true)) {
                dc.wire().write("FirstName").text("Quartilla");
            }

            assertEquals(index, appender.lastIndexAppended());
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

            ExcerptTailer tailer = chronicle.createTailer(named ? "named" : null);

            try (DocumentContext dc = tailer.readingDocument()) {

                assertEquals(myMarshable, dc.wire().read("myMarshable").typedMarshallable());
            }
        }
    }

    @Test
    public void testMetaData() {
        assumeFalse(named);
        try (final ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .build()) {

            final ExcerptAppender appender = chronicle.acquireAppender();

            try (DocumentContext dc = appender.writingDocument(true)) {
                dc.wire().write("FirstName").text("Quartilla");
            }

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write("FirstName").text("Rob");
            }

            try (DocumentContext dc = appender.writingDocument(true)) {
                dc.wire().write("FirstName").text("Steve");
            }

            final ExcerptTailer tailer = chronicle.createTailer(named ? "named" : null);

            StringBuilder event = new StringBuilder();
            while (true) {
                try (DocumentContext dc = tailer.readingDocument(true)) {
                    assertTrue(dc.isMetaData());
                    ValueIn in = dc.wire().read(event);
                    // first we will pick up header, index etc.
                    if (!StringUtils.isEqual(event, "FirstName"))
                        continue;

                    in.text("Quartilla", Assert::assertEquals);
                    break;
                }
            }

            long robIndex;
            try (DocumentContext dc = tailer.readingDocument(true)) {
                assertTrue(dc.isData());
                robIndex = dc.index();
                dc.wire().read("FirstName").text("Rob", Assert::assertEquals);
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

            assertTrue(tailer.moveToIndex(robIndex));
            try (DocumentContext dc = tailer.readingDocument(false)) {
                assertTrue(dc.isData());
                dc.wire().read("FirstName").text("Rob", Assert::assertEquals);
            }
        }
    }

    @Test
    public void testReadingSecondDocumentNotExist() {
        try (final ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .build()) {

            final ExcerptAppender appender = chronicle.acquireAppender();

            try (DocumentContext dc = appender.writingDocument()) {

                dc.wire().write("FirstName").text("Quartilla");
            }

            final ExcerptTailer tailer = chronicle.createTailer(named ? "named" : null);

            try (DocumentContext dc = tailer.readingDocument()) {
                String text = dc.wire().read("FirstName").text();
                assertEquals("Quartilla", text);
            }

            try (DocumentContext dc = tailer.readingDocument()) {
                assertFalse(dc.isPresent());
            }
        }
    }

    @Test
    public void testDocumentIndexTest() {
        try (final ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .build()) {

            final ExcerptAppender appender = chronicle.acquireAppender();

            try (DocumentContext dc = appender.writingDocument()) {
                long index = dc.index();
                assertEquals(0, chronicle.rollCycle().toSequenceNumber(index));
                dc.wire().write("FirstName").text("Quartilla");
            }

            try (DocumentContext dc = appender.writingDocument()) {
                assertEquals(1, chronicle.rollCycle().toSequenceNumber(dc.index()));
                dc.wire().write("FirstName").text("Rob");
            }

            try (DocumentContext dc = appender.writingDocument()) {
                assertEquals(2, chronicle.rollCycle().toSequenceNumber(dc.index()));
                dc.wire().write("FirstName").text("Rob");
            }

            ExcerptTailer tailer = chronicle.createTailer(named ? "named" : null);

            try (DocumentContext dc = tailer.readingDocument()) {
                long index = dc.index();
                assertEquals(0, chronicle.rollCycle().toSequenceNumber(index));

            }

            try (DocumentContext dc = tailer.readingDocument()) {
                assertEquals(1, chronicle.rollCycle().toSequenceNumber(dc.index()));

            }

            try (DocumentContext dc = tailer.readingDocument()) {
                assertEquals(2, chronicle.rollCycle().toSequenceNumber(dc.index()));

            }
        }
    }

    @Test
    public void testReadingSecondDocumentNotExistIncludingMeta() {
        try (final ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .build()) {

            final ExcerptAppender appender = chronicle.acquireAppender();

            try (DocumentContext dc = appender.writingDocument()) {

                dc.wire().write("FirstName").text("Quartilla");
            }

            final ExcerptTailer tailer = chronicle.createTailer(named ? "named" : null);
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
            Bytes<?> steve = Bytes.allocateDirect("Steve".getBytes());
            appender.writeBytes(steve);
            Bytes<?> jobs = Bytes.allocateDirect("Jobs".getBytes());
            appender.writeBytes(jobs);

            final ExcerptTailer tailer = chronicle.createTailer(named ? "named" : null);
            Bytes<?> bytes = Bytes.elasticByteBuffer();
            try {
                tailer.readBytes(bytes);
                assertEquals("Steve", bytes.toString());
                bytes.clear();
                tailer.readBytes(bytes);
                assertEquals("Jobs", bytes.toString());
            } finally {
                steve.releaseLast();
                jobs.releaseLast();
                bytes.releaseLast();
            }
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
                    context.wire().write("key").text("value=" + j);
                }
            }
            long lastIndex = appender.lastIndexAppended();

            final int cycle = queue.rollCycle().toCycle(lastIndex);
            assertEquals(queue.firstCycle(), cycle);
            assertEquals(queue.lastCycle(), cycle);
            final ExcerptTailer tailer = queue.createTailer(named ? "named" : null);

            StringBuilder sb = new StringBuilder();

            for (int i : new int[]{0, 8, 7, 9, 64, 65, 66}) {
                final long index = queue.rollCycle().toIndex(cycle, i);
                assertTrue("i: " + i,
                        tailer.moveToIndex(
                                index));
                final DocumentContext context = tailer.readingDocument();
                assertEquals(index, context.index());
                context.wire().read("key").text(sb);
                assertEquals("value=" + i, sb.toString());
            }
        }
    }

    @Ignore("long running test")
    @Test
    public void testReadAtIndex4MB() {
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.builder(getTmpDir(), this.wireType).rollCycle(SMALL_DAILY)
                .build()) {
            final ExcerptAppender appender = queue.acquireAppender();

            for (long i = 0; i < TIMES; i++) {
                final long j = i;
                appender.writeDocument(wire -> wire.write("key").text("value=" + j));
            }
            long lastIndex = appender.lastIndexAppended();

            final int cycle = queue.rollCycle().toCycle(lastIndex);

            final ExcerptTailer tailer = queue.createTailer(named ? "named" : null);

            StringBuilder sb = new StringBuilder();

            for (long i = 0; i < (4L << 20L); i++) {
                assertTrue(tailer.moveToIndex(queue.rollCycle().toIndex(cycle, i)));
                tailer.readDocument(wire -> wire.read("key").text(sb));
                assertEquals("value=" + i, sb.toString());
            }
        }
    }

    @Test
    public void testMetaIndexTest() {

        File tmpDir = getTmpDir();
        try (final ChronicleQueue q = builderWithAppendListener(tmpDir, wireType).rollCycle(HOURLY).build()) {
            {
                ExcerptAppender appender = q.acquireAppender();
                try (DocumentContext documentContext = appender.writingDocument()) {
                    documentContext.wire().getValueOut().text("one");
                }
                try (DocumentContext documentContext = appender.writingDocument()) {
                    documentContext.wire().getValueOut().text("two");
                }
                try (DocumentContext documentContext = appender.writingDocument(true)) {
                    documentContext.wire().getValueOut().text("meta1");
                }

                try (DocumentContext documentContext = appender.writingDocument()) {
                    documentContext.wire().getValueOut().text("three");
                }

                try (DocumentContext documentContext = appender.writingDocument(true)) {
                    documentContext.wire().getValueOut().text("meta2");
                }
                try (DocumentContext documentContext = appender.writingDocument(true)) {
                    documentContext.wire().getValueOut().text("meta3");
                }
                try (DocumentContext documentContext = appender.writingDocument()) {
                    documentContext.wire().getValueOut().text("four");
                }
            }
            {

                ExcerptTailer tailer = q.createTailer(named ? "named" : null);

                try (DocumentContext documentContext2 = tailer.readingDocument()) {
                    assertEquals(0, toSeq(q, documentContext2.index()));
                    assertFalse(documentContext2.isMetaData());
                    assertEquals("one", documentContext2.wire().getValueIn().text());
                }

                try (DocumentContext documentContext2 = tailer.readingDocument(true)) {
                    assertEquals(1, toSeq(q, documentContext2.index()));
                    assertFalse(documentContext2.isMetaData());
                    assertEquals("two", documentContext2.wire().getValueIn().text());
                }

                try (DocumentContext documentContext2 = tailer.readingDocument(true)) {
                    assertEquals(2, toSeq(q, documentContext2.index()));
                    assertTrue(documentContext2.isMetaData());
                    assertEquals("meta1", documentContext2.wire().getValueIn().text());
                }

                try (DocumentContext documentContext2 = tailer.readingDocument(true)) {
                    assertEquals(2, toSeq(q, documentContext2.index()));
                    assertFalse(documentContext2.isMetaData());
                    assertEquals("three", documentContext2.wire().getValueIn().text());
                }

                try (DocumentContext documentContext2 = tailer.readingDocument(true)) {
                    assertEquals(3, toSeq(q, documentContext2.index()));
                    assertTrue(documentContext2.isMetaData());
                    assertEquals("meta2", documentContext2.wire().getValueIn().text());
                }

                try (DocumentContext documentContext2 = tailer.readingDocument(true)) {
                    assertEquals(3, toSeq(q, documentContext2.index()));
                    assertTrue(documentContext2.isMetaData());
                    assertEquals("meta3", documentContext2.wire().getValueIn().text());
                }

                try (DocumentContext documentContext2 = tailer.readingDocument(true)) {
                    assertEquals(3, toSeq(q, documentContext2.index()));
                    assertFalse(documentContext2.isMetaData());
                    assertEquals("four", documentContext2.wire().getValueIn().text());
                }
            }

            {
                ExcerptTailer tailer = q.createTailer(named ? "named2" : null);

                try (DocumentContext documentContext2 = tailer.readingDocument()) {
                    assertEquals(0, toSeq(q, documentContext2.index()));
                    assertFalse(documentContext2.isMetaData());
                    assertEquals("one", documentContext2.wire().getValueIn().text());
                }

                try (DocumentContext documentContext2 = tailer.readingDocument(false)) {
                    assertEquals(1, toSeq(q, documentContext2.index()));
                    assertFalse(documentContext2.isMetaData());
                    assertEquals("two", documentContext2.wire().getValueIn().text());
                }

                try (DocumentContext documentContext2 = tailer.readingDocument(false)) {
                    assertEquals(2, toSeq(q, documentContext2.index()));
                    assertFalse(documentContext2.isMetaData());
                    assertEquals("three", documentContext2.wire().getValueIn().text());
                }
            }
        }
        assertEquals("" +
                        "idx: 6f06c00000000\n" +
                        "# position: 65808, header: 0\n" +
                        "--- !!data #binary\n" +
                        "one\n" +
                        "\n" +
                        "idx: 6f06c00000001\n" +
                        "# position: 65816, header: 0\n" +
                        "--- !!data #binary\n" +
                        "two\n" +
                        "\n" +
                        "idx: 6f06c00000002\n" +
                        "# position: 65836, header: 0\n" +
                        "--- !!data #binary\n" +
                        "three\n" +
                        "\n" +
                        "idx: 6f06c00000003\n" +
                        "# position: 65872, header: 0\n" +
                        "--- !!data #binary\n" +
                        "four\n" +
                        "\n",
                appenderListenerDump.toString());
    }

    @Test
    public void testLastWrittenIndexPerAppender() {
        try (final ChronicleQueue queue = builder(getTmpDir(), this.wireType)
                .build()) {
            final ExcerptAppender appender = queue.acquireAppender();

            appender.writeDocument(wire -> wire.write("key").text("test"));
            assertEquals(0, queue.rollCycle().toSequenceNumber(appender.lastIndexAppended()));
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testLastWrittenIndexPerAppenderNoData() {
        try (final ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .build()) {
            final ExcerptAppender appender = chronicle.acquireAppender();
            appender.lastIndexAppended();
            fail();
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
        try (final ChronicleQueue queue = builder(getTmpDir(), this.wireType)
                .build()) {

            final ExcerptAppender appender = queue.acquireAppender();
            final int cycle = appender.cycle();
            // create 100 documents
            for (int i = 0; i < 100; i++) {
                final int j = i;
                appender.writeDocument(wire -> wire.write("key").text("value=" + j));
            }

            final ExcerptTailer tailer = queue.createTailer(named ? "named" : null);
            assertTrue(tailer.moveToIndex(queue.rollCycle().toIndex(cycle, 0)));

            StringBuilder sb = new StringBuilder();
            tailer.readDocument(wire -> wire.read("key").text(sb));

            assertEquals("value=0", sb.toString());
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
            appender.writeDocument(wire -> wire.write("key").text("value=v"));
            assertEquals(0, appender.cycle());
        }
    }

    @Test
    public void shouldBeAbleToReadFromQueueWithNonZeroEpoch() {
        try (final ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .epoch(System.currentTimeMillis())
                .rollCycle(RollCycles.DEFAULT)
                .build()) {

            final ExcerptAppender appender = chronicle.acquireAppender();
            appender.writeDocument(wire -> wire.write("key").text("value=v"));
            assertEquals(0, appender.cycle());

            final ExcerptTailer excerptTailer = chronicle.createTailer(named ? "named" : null).toStart();
            assertTrue(excerptTailer.readingDocument().isPresent());
        }
    }

    @Test
    public void shouldHandleLargeEpoch() {
        try (final ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .epoch(System.currentTimeMillis())
                .epoch(1284739200000L)
                .rollCycle(DEFAULT)
                .build()) {

            final ExcerptAppender appender = chronicle.acquireAppender();
            appender.writeDocument(wire -> wire.write("key").text("value=v"));

            final ExcerptTailer excerptTailer = chronicle.createTailer(named ? "named" : null).toStart();
            assertTrue(excerptTailer.readingDocument().isPresent());
        }
    }

    @Test
    public void testNegativeEPOC() {
        for (int h = -14; h <= 14; h++) {
            try (final ChronicleQueue chronicle = builder(getTmpDir(), wireType)
                    .epoch(TimeUnit.HOURS.toMillis(h))
                    .build()) {

                final ExcerptAppender appender = chronicle.acquireAppender();
                appender.writeDocument(wire -> wire.write("key").text("value=v"));
                chronicle.createTailer(named ? "named" : null)
                        .readDocument(wire -> {
                            assertEquals("value=v", wire.read("key").text());
                        });
            }
        }
    }

    @Test
    public void testIndex() {
        try (final ChronicleQueue queue = builder(getTmpDir(), this.wireType)
                .rollCycle(RollCycles.HOURLY)
                .build()) {

            final ExcerptAppender appender = queue.acquireAppender();
            int cycle = appender.cycle();

            // create 100 documents
            for (int i = 0; i < 5; i++) {
                final int j = i;
                appender.writeDocument(wire -> wire.write("key").text("value=" + j));
                if (i == 2) {
                    final long cycle1 = queue.rollCycle().toCycle(appender.lastIndexAppended());
                    assertEquals(cycle1, cycle);
                }
            }

            final ExcerptTailer tailer = queue.createTailer(named ? "named" : null);
            assertTrue(tailer.moveToIndex(queue.rollCycle().toIndex(cycle, 2)));

            StringBuilder sb = new StringBuilder();
            tailer.readDocument(wire -> wire.read("key").text(sb));
            assertEquals("value=2", sb.toString());

            tailer.readDocument(wire -> wire.read("key").text(sb));
            assertEquals("value=3", sb.toString());

            tailer.readDocument(wire -> wire.read("key").text(sb));
            assertEquals("value=4", sb.toString());
        }
    }

    @Test
    public void testReadingDocument() {
        try (final ChronicleQueue queue = builder(getTmpDir(), this.wireType)
                .rollCycle(RollCycles.HOURLY)
                .build()) {

            final ExcerptAppender appender = queue.acquireAppender();
            long cycle = appender.cycle();

            // create 100 documents
            for (int i = 0; i < 5; i++) {
                final int j = i;
                appender.writeDocument(wire -> wire.write("key").text("value=" + j));
                if (i == 2) {
                    final long cycle1 = queue.rollCycle().toCycle(appender.lastIndexAppended());
                    assertEquals(cycle1, cycle);
                }
            }

            final ExcerptTailer tailer = queue.createTailer(named ? "named" : null);

            final StringBuilder sb = Wires.acquireStringBuilder();

            try (final DocumentContext dc = tailer.readingDocument()) {
                assert dc.isPresent();
                assert dc.isData();
                dc.wire().read("key").text(sb);
                assertEquals("value=0", sb.toString());
            }

            try (final DocumentContext dc = tailer.readingDocument()) {
                assert dc.isPresent();
                assert dc.isData();
                dc.wire().read("key").text(sb);
                assertEquals("value=1", sb.toString());
            }

            try (final DocumentContext dc = tailer.readingDocument()) {
                assert dc.isPresent();
                assert dc.isData();
                dc.wire().read("key").text(sb);
                assertEquals("value=2", sb.toString());
            }

            try (final DocumentContext dc = tailer.readingDocument()) {
                assert dc.isPresent();
                assert dc.isData();
                dc.wire().read("key").text(sb);
                assertEquals("value=3", sb.toString());
            }

            try (final DocumentContext dc = tailer.readingDocument()) {
                assert dc.isPresent();
                assert dc.isData();
                dc.wire().read("key").text(sb);
                assertEquals("value=4", sb.toString());
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

        try (final ChronicleQueue queue = builder(getTmpDir(), this.wireType)
                .rollCycle(RollCycles.HOURLY)
                .build()) {

            final ExcerptAppender appender = queue.acquireAppender();
            int cycle = appender.cycle();

            // create 100 documents
            for (int i = 0; i < 5; i++) {
                final int j = i;
                appender.writeDocument(wire -> wire.write("key").text("value=" + j));
                if (i == 2) {
                    final long cycle1 = queue.rollCycle().toCycle(appender.lastIndexAppended());
                    assertEquals(cycle1, cycle);
                }
            }

            final ExcerptTailer tailer = queue.createTailer(named ? "named" : null);
            assertTrue(tailer.moveToIndex(queue.rollCycle().toIndex(cycle, 2)));

            final StringBuilder sb = Wires.acquireStringBuilder();

            try (final DocumentContext dc = tailer.readingDocument()) {
                assert dc.isPresent();
                assert dc.isData();
                dc.wire().read("key").text(sb);
                assertEquals("value=2", sb.toString());
            }

            try (final DocumentContext dc = tailer.readingDocument()) {
                assert dc.isPresent();
                assert dc.isData();
                dc.wire().read("key").text(sb);
                assertEquals("value=3", sb.toString());
            }

            try (final DocumentContext dc = tailer.readingDocument()) {
                assert dc.isPresent();
                assert dc.isData();
                dc.wire().read("key").text(sb);
                assertEquals("value=4", sb.toString());
            }

            try (final DocumentContext dc = tailer.readingDocument()) {
                assert !dc.isPresent();
                assert !dc.isData();
                assert !dc.isMetaData();
            }
        }
    }

    @Test
    public void testReadingDocumentWithFirstAMoveWithEpoch() {
        Instant hourly = Instant.parse("2018-02-12T00:59:59.999Z");
        Instant minutely = Instant.parse("2018-02-12T00:00:59.999Z");

        Date epochHourlyFirstCycle = Date.from(hourly);
        Date epochMinutelyFirstCycle = Date.from(minutely);
        Date epochHourlySecondCycle = Date.from(hourly.plusMillis(1));
        Date epochMinutelySecondCycle = Date.from(minutely.plusMillis(1));

        doTestEpochMove(epochHourlyFirstCycle.getTime(), RollCycles.MINUTELY);
        doTestEpochMove(epochHourlySecondCycle.getTime(), RollCycles.MINUTELY);
        doTestEpochMove(epochHourlyFirstCycle.getTime(), RollCycles.HOURLY);
        doTestEpochMove(epochHourlySecondCycle.getTime(), RollCycles.HOURLY);
        doTestEpochMove(epochHourlyFirstCycle.getTime(), RollCycles.DAILY);
        doTestEpochMove(epochHourlySecondCycle.getTime(), RollCycles.DAILY);

        doTestEpochMove(epochMinutelyFirstCycle.getTime(), RollCycles.MINUTELY);
        doTestEpochMove(epochMinutelySecondCycle.getTime(), RollCycles.MINUTELY);
        doTestEpochMove(epochMinutelyFirstCycle.getTime(), RollCycles.HOURLY);
        doTestEpochMove(epochMinutelySecondCycle.getTime(), RollCycles.HOURLY);
        doTestEpochMove(epochMinutelyFirstCycle.getTime(), RollCycles.DAILY);
        doTestEpochMove(epochMinutelySecondCycle.getTime(), RollCycles.DAILY);
    }

    private void doTestEpochMove(long epoch, RollCycle rollCycle) {
        try (final ChronicleQueue queue = builder(getTmpDir(), this.wireType)
                .rollCycle(rollCycle)
                .epoch(epoch)
                .build()) {

            final ExcerptAppender appender = queue.acquireAppender();
            int cycle = appender.cycle();

            // create 100 documents
            int last = 4;
            for (int i = 0; i <= last; i++) {
                final int j = i;
                appender.writeDocument(wire -> wire.write("key").text("value=" + j));
                if (i == last) {
                    final long cycle1 = queue.rollCycle().toCycle(appender.lastIndexAppended());
                    if (cycle + 1 != cycle1)
                        assertEquals(cycle, cycle1);
                }
            }

            final ExcerptTailer tailer = queue.createTailer(named ? "named" : null);
            assertTrue(tailer.moveToIndex(queue.rollCycle().toIndex(cycle, 2)));

            final StringBuilder sb = Wires.acquireStringBuilder();

            try (final DocumentContext dc = tailer.readingDocument()) {
                assert dc.isPresent();
                assert dc.isData();
                dc.wire().read("key").text(sb);
                assertEquals("value=2", sb.toString());
            }

            try (final DocumentContext dc = tailer.readingDocument()) {
                assert dc.isPresent();
                assert dc.isData();
                dc.wire().read("key").text(sb);
                assertEquals("value=3", sb.toString());
            }

            try (final DocumentContext dc = tailer.readingDocument()) {
                assert dc.isPresent();
                assert dc.isData();
                dc.wire().read("key").text(sb);
                assertEquals("value=4", sb.toString());
            }

            try (final DocumentContext dc = tailer.readingDocument()) {
                assert !dc.isPresent();
                assert !dc.isData();
                assert !dc.isMetaData();
            }
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
            ExcerptTailer tailer = chronicle.createTailer(named ? "named" : null);

            ExcerptAppender append = chronicle2.acquireAppender();
            append.writeDocument(w -> w.write("test").text("text"));

            while (tailer.state() == TailerState.UNINITIALISED)
                tailer.toEnd();

            try (DocumentContext dc = tailer.readingDocument()) {
                assertFalse(tailer.index() + " " + tailer.state(), dc.isPresent());
            }

            append.writeDocument(w -> w.write("test").text("text2"));
            try (DocumentContext dc = tailer.readingDocument()) {
                assertTrue(dc.isPresent());

                assertEquals("text2", dc.wire().read("test").text());
            }
        }
    }

    @Test
    public void testReentrant() {

        File tmpDir = getTmpDir();
        try (final ChronicleQueue queue = binary(tmpDir)
                .testBlockSize()
                .rollCycle(RollCycles.TEST_DAILY)
                .timeProvider(new SetTimeProvider("2020/10/19T01:01:01"))
                .build()) {
            ExcerptAppender appender = queue.acquireAppender();

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write("some").text("data");

                try (DocumentContext dc2 = appender.writingDocument()) {
                    dc2.wire().write("some2").text("other");
                }
                assertTrue(dc.isOpen());
            }
            assertEquals("" +
                    "--- !!meta-data #binary\n" +
                    "header: !STStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  metadata: !SCQMeta {\n" +
                    "    roll: !SCQSRoll { length: 86400000, format: yyyyMMdd'T1', epoch: 0 },\n" +
                    "    deltaCheckpointInterval: 64,\n" +
                    "    sourceId: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "--- !!data #binary\n" +
                    "listing.highestCycle: 18554\n" +
                    "--- !!data #binary\n" +
                    "listing.lowestCycle: 18554\n" +
                    "--- !!data #binary\n" +
                    "listing.modCount: 3\n" +
                    queueLockForTestReentrant() +
                    "--- !!data #binary\n" +
                    "chronicle.write.lock: -9223372036854775808\n" +
                    "--- !!data #binary\n" +
                    "chronicle.append.lock: -9223372036854775808\n" +
                    "--- !!data #binary\n" +
                    "chronicle.lastIndexReplicated: -1\n" +
                    "--- !!data #binary\n" +
                    "chronicle.lastAcknowledgedIndexReplicated: -1\n" +
                    "...\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  writePosition: [\n" +
                    "    400,\n" +
                    "    1717986918400\n" +
                    "  ],\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 200,\n" +
                    "    lastIndex: 1\n" +
                    "  },\n" +
                    "  dataFormat: 1\n" +
                    "}\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  304,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  400,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "--- !!data #binary\n" +
                    "some: data\n" +
                    "some2: other\n" +
                    "...\n", queue.dump().replaceAll("(?m)^#.+$\\n", ""));
        }
    }

    protected String queueLockForTestReentrant() {
        return "";
    }

    @Test
    public void testToEnd() throws InterruptedException {
        File dir = getTmpDir();
        try (ChronicleQueue queue = builder(dir, wireType)
                .rollCycle(RollCycles.HOURLY)
                .build()) {
            ExcerptTailer tailer = queue.createTailer(named ? "named" : null);

            // move to the end even though it doesn't exist yet.
            tailer.toEnd();

            try (ChronicleQueue chronicle2 = builder(dir, wireType)
                    .rollCycle(RollCycles.HOURLY)
                    .build()) {

                ExcerptAppender append = chronicle2.acquireAppender();
                append.writeDocument(w -> w.write("test").text("text"));

            }
            // this is needed to avoid caching of first and last cycle, see SingleChronicleQueue#setFirstAndLastCycle
            Thread.sleep(1);

            try (DocumentContext dc = tailer.readingDocument()) {
                try (final SingleChronicleQueue build = builder(dir, wireType).rollCycle(HOURLY).build()) {
                    String message = "dump: " + build.dump();
                    assertTrue(message, dc.isPresent());
                    assertEquals(message, "text", dc.wire().read("test").text());
                }
            }
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
            append.writeDocument(w -> w.write("test").text("before text"));

            ExcerptTailer tailer = chronicle.createTailer(named ? "named" : null);

            // move to the end even though it doesn't exist yet.
            tailer.toEnd();

            append.writeDocument(w -> w.write("test").text("text"));

            assertTrue(tailer.readDocument(w -> w.read("test").text("text", Assert::assertEquals)));
        }
    }

    @Test
    public void testToEndOnDeletedQueueFiles() throws IOException {
        if (OS.isWindows()) {
            System.err.println("#460 Cannot test delete after close on windows");
            return;
        }

        File dir = getTmpDir();
        try (ChronicleQueue q = builder(dir, wireType).build()) {
            ExcerptAppender append = q.acquireAppender();
            append.writeDocument(w -> w.write("test").text("before text"));

            ExcerptTailer tailer = q.createTailer(named ? "named" : null);

            // move to the end even though it doesn't exist yet.
            tailer.toEnd();

            append.writeDocument(w -> w.write("test").text("text"));

            assertTrue(tailer.readDocument(w -> w.read("test").text("text", Assert::assertEquals)));

            try (Stream<Path> cq4Files = Files.find(dir.toPath(), 1, (p, basicFileAttributes) -> p.toString().endsWith("cq4"), FileVisitOption.FOLLOW_LINKS)) {
                final List<Path> unDeletable = cq4Files.filter(path -> !path.toFile().delete())
                        .collect(Collectors.toList());
                assertTrue("Unable to delete" + unDeletable, unDeletable.isEmpty());
            }

            try (ChronicleQueue q2 = builder(dir, wireType).build()) {
                tailer = q2.createTailer(named ? "named" : null);
                tailer.toEnd();
                assertEquals(TailerState.UNINITIALISED, tailer.state());
                append = q2.acquireAppender();
                append.writeDocument(w -> w.write("test").text("before text"));

                assertTrue(tailer.readDocument(w -> w.read("test").text("before text", Assert::assertEquals)));
            }
        }
    }

    @Test
    public void testReadWrite() {
        File dir = getTmpDir();
        try (ChronicleQueue chronicle = builder(dir, wireType)
                .rollCycle(RollCycles.HOURLY)
                .testBlockSize()
                .build();
             ChronicleQueue chronicle2 = builder(dir, wireType)
                     .rollCycle(RollCycles.HOURLY)
                     .testBlockSize()
                     .build()) {
            ExcerptAppender append = chronicle2.acquireAppender();
            int runs = 50_000;
            for (int i = 0; i < runs; i++) {
                append.writeDocument(w -> w
                        .write("test - message")
                        .text("text"));
            }

            ExcerptTailer tailer = chronicle.createTailer(named ? "named1" : null);
            ExcerptTailer tailer2 = chronicle.createTailer(named ? "named2" : null);
            ExcerptTailer tailer3 = chronicle.createTailer(named ? "named3" : null);
            ExcerptTailer tailer4 = chronicle.createTailer(named ? "named4" : null);
            for (int i = 0; i < runs; i++) {
                if (i % 10000 == 0)
                    System.gc();
                if (i % 2 == 0)
                    assertTrue(tailer2.readDocument(w -> w.read("test - message").text("text", Assert::assertEquals)));
                if (i % 3 == 0)
                    assertTrue(tailer3.readDocument(w -> w.read("test - message").text("text", Assert::assertEquals)));
                if (i % 4 == 0)
                    assertTrue(tailer4.readDocument(w -> w.read("test - message").text("text", Assert::assertEquals)));
                assertTrue(tailer.readDocument(w -> w.read("test - message").text("text", Assert::assertEquals)));
            }
        }
    }

    @Test
    public void testReadingDocumentForEmptyQueue() {
        File dir = getTmpDir();
        try (ChronicleQueue chronicle = builder(dir, this.wireType)
                .rollCycle(RollCycles.HOURLY)
                .build()) {
            ExcerptTailer tailer = chronicle.createTailer(named ? "named" : null);
            // DocumentContext is empty as we have no queue and don't know what the wire type will be.
            try (DocumentContext dc = tailer.readingDocument()) {
                assertFalse(dc.isPresent());
            }

            try (ChronicleQueue chronicle2 = builder(dir, this.wireType)
                    .rollCycle(RollCycles.HOURLY)
                    .build()) {
                ExcerptAppender appender = chronicle2.acquireAppender();
                appender.writeDocument(w -> w.write("test - message").text("text"));

                while (tailer.state() == TailerState.UNINITIALISED)
                    tailer.toStart();

                // DocumentContext should not be empty as we know what the wire type will be.
                try (DocumentContext dc = tailer.readingDocument()) {
                    assertTrue(dc.isPresent());
                    dc.wire().read("test - message").text("text", Assert::assertEquals);
                }
            }
        }
    }

    @Test
    public void testMetaData6() {
        assumeFalse(named);
        try (final ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .rollCycle(TEST2_DAILY)
                .timeProvider(new SetTimeProvider("2020/10/19T01:01:01"))
                .build()) {

            final ExcerptAppender appender = chronicle.acquireAppender();

            try (DocumentContext dc = appender.writingDocument(true)) {
                dc.wire().write("FirstName").text("Quartilla");
            }

            try (DocumentContext dc = appender.writingDocument()) {
                assertFalse(dc.isMetaData());
                dc.wire().write("FirstName").text("Helen");
            }
            try (DocumentContext dc = appender.writingDocument(true)) {
                dc.wire().write("FirstName").text("Steve");
            }

            final ExcerptTailer tailer = chronicle.createTailer(named ? "named" : null);

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
                dc.wire().read("FirstName").text("Helen", Assert::assertEquals);
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
            assertEquals(expectedMetaDataTest2(), chronicle.dump().replaceAll("(?m)^#.+$\\n", ""));
        }
    }

    @NotNull
    protected String expectedMetaDataTest2() {
        if (wireType == WireType.BINARY || wireType == WireType.BINARY_LIGHT || wireType == WireType.COMPRESSED_BINARY)
            return "" +
                    "--- !!meta-data #binary\n" +
                    "header: !STStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  metadata: !SCQMeta {\n" +
                    "    roll: !SCQSRoll { length: 86400000, format: yyyyMMdd'T2', epoch: 0 },\n" +
                    "    deltaCheckpointInterval: 64,\n" +
                    "    sourceId: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "--- !!data #binary\n" +
                    "listing.highestCycle: 18554\n" +
                    "--- !!data #binary\n" +
                    "listing.lowestCycle: 18554\n" +
                    "--- !!data #binary\n" +
                    "listing.modCount: 3\n" +
                    "--- !!data #binary\n" +
                    "chronicle.write.lock: -9223372036854775808\n" +
                    "--- !!data #binary\n" +
                    "chronicle.append.lock: -9223372036854775808\n" +
                    "--- !!data #binary\n" +
                    "chronicle.lastIndexReplicated: -1\n" +
                    "--- !!data #binary\n" +
                    "chronicle.lastAcknowledgedIndexReplicated: -1\n" +
                    "...\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  writePosition: [\n" +
                    "    552,\n" +
                    "    2370821947392\n" +
                    "  ],\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 16,\n" +
                    "    indexSpacing: 2,\n" +
                    "    index2Index: 200,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  dataFormat: 1\n" +
                    "}\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 16, used: 1\n" +
                    "  368,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 16, used: 1\n" +
                    "  552,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "--- !!meta-data #binary\n" +
                    "FirstName: Quartilla\n" +
                    "--- !!data #binary\n" +
                    "FirstName: Helen\n" +
                    "--- !!meta-data #binary\n" +
                    "FirstName: Steve\n" +
                    "...\n";

        throw new IllegalStateException("unknown type " + wireType);
    }

    @Test(expected = IllegalArgumentException.class)
    public void dontPassQueueToReader() {
        try (ChronicleQueue queue = binary(getTmpDir()).build()) {
            queue.createTailer(named ? "named" : null).afterLastWritten(queue).methodReader();
        }
    }

    @Test
    public void testToEndBeforeWrite() {
        try (ChronicleQueue chronicle = builder(getTmpDir(), wireType)
                .rollCycle(TEST2_DAILY)
                .build();
             ExcerptAppender appender = chronicle.acquireAppender();
             ExcerptTailer tailer = chronicle.createTailer(named ? "named" : null)) {

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
    public void testForwardFollowedBackBackwardTailer() {
        try (ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
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
        try (final ChronicleQueue queue = builder(getTmpDir(), this.wireType)
                .rollCycle(TEST2_DAILY)
                .build()) {

            final ExcerptAppender appender = queue.acquireAppender();
            appender.writeDocument(w -> w.writeEventName("hello").text("world"));

            final ExcerptTailer tailer = queue.createTailer(named ? "named" : null);
            tailer.toEnd();
            tailer.direction(TailerDirection.BACKWARD);

            assertTrue(tailer.readingDocument().isPresent());
        }
    }

    void readForward(@NotNull ChronicleQueue chronicle, int entries) {
        try (ExcerptTailer forwardTailer = chronicle.createTailer(named ? "named" : null)
                .direction(TailerDirection.FORWARD)
                .toStart()) {

            for (int i = 0; i < entries; i++) {
                try (DocumentContext documentContext = forwardTailer.readingDocument()) {
                    assertTrue(documentContext.isPresent());
                    assertEquals(i, RollCycles.DEFAULT.toSequenceNumber(documentContext.index()));
                    StringBuilder sb = Wires.acquireStringBuilder();
                    ValueIn valueIn = documentContext.wire().readEventName(sb);
                    assertTrue("hello".contentEquals(sb));
                    String actual = valueIn.text();
                    assertEquals("world" + i, actual);
                }
            }
            try (DocumentContext documentContext = forwardTailer.readingDocument()) {
                assertFalse(documentContext.isPresent());
            }
        }
    }

    void readBackward(@NotNull ChronicleQueue chronicle, int entries) {
        ExcerptTailer backwardTailer = chronicle.createTailer(named ? "named" : null)
                .direction(TailerDirection.BACKWARD)
                .toEnd();

        for (int i = entries - 1; i >= 0; i--) {
            try (DocumentContext documentContext = backwardTailer.readingDocument()) {
                assertTrue(documentContext.isPresent());
                final long index = documentContext.index();
                assertEquals("index: " + index, i, (int) index);
                assertEquals(i, RollCycles.DEFAULT.toSequenceNumber(index));
                assertTrue(documentContext.isPresent());
                StringBuilder sb = Wires.acquireStringBuilder();
                ValueIn valueIn = documentContext.wire().readEventName(sb);
                assertTrue("hello".contentEquals(sb));
                String actual = valueIn.text();
                assertEquals("world" + i, actual);
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

        try (ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .rollCycle(cycle)
                .timeProvider(timeProvider)
                .build()) {

            ExcerptAppender appender = chronicle.acquireAppender();
            appender.writeDocument(w -> w.writeEventName("hello").text("world"));

            // go to the cycle next to the one the write was made on
            forwardToFuture.set(true);

            ExcerptTailer forwardTailer = chronicle.createTailer(named ? "named" : null)
                    .direction(TailerDirection.FORWARD)
                    .toStart();

            try (DocumentContext context = forwardTailer.readingDocument()) {
                assertTrue(context.isPresent());
            }
            try (DocumentContext context = forwardTailer.readingDocument()) {
                assertFalse(context.isPresent());
            }

            ExcerptTailer backwardTailer = chronicle.createTailer(named ? "named" : null)
                    .direction(TailerDirection.BACKWARD)
                    .toEnd();

            try (DocumentContext context = backwardTailer.readingDocument()) {
                assertTrue(context.isPresent());
            }
        }
    }

    @Test
    public void testSomeMessages() {
        try (ChronicleQueue chronicle = builder(getTmpDir(), wireType)
                .rollCycle(TEST2_DAILY)
                .build()) {

            ExcerptAppender appender = chronicle.acquireAppender();
            ExcerptTailer tailer = chronicle.createTailer(named ? "named" : null);

            int entries = chronicle.rollCycle().defaultIndexSpacing() * 2 + 2;

            for (long i = 0; i < entries; i++) {
                long finalI = i;
                appender.writeDocument(w -> w.writeEventName("hello").int64(finalI));
                long seq = chronicle.rollCycle().toSequenceNumber(appender.lastIndexAppended());
                assertEquals(i, seq);
                // System.out.println(chronicle.dump());
                tailer.readDocument(w -> w.read().int64(finalI, (a, b) -> assertEquals((long) a, b)));
            }
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
            // System.out.println(chronicle.dump());
            ExcerptTailer tailer = chronicle.createTailer(named ? "named" : null);
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
            File name2 = getTmpDir();
            try (ChronicleQueue chronicle = builder(name2, this.wireType)
                    .build()) {

                ExcerptAppender appender = chronicle.acquireAppender();
                appender.writeDocument(w -> w.writeEventName("hello").text("world0"));
                appender.writeDocument(w -> w.writeEventName("hello").text("world1"));
                appender.writeDocument(w -> w.writeEventName("hello").text("world2"));

                ExcerptTailer tailer = chronicle.createTailer(named ? "named" : null);

                try (DocumentContext documentContext = tailer.readingDocument()) {
                    sync.writeBytes(documentContext.index(), documentContext.wire().bytes());
                }
                try (DocumentContext documentContext = tailer.readingDocument()) {
                    String text = documentContext.wire().read().text();
                    assertEquals("world1", text);
                }
            }
        }
    }

    @Test
    public void testMapWrapper() {
        try (ChronicleQueue syncQ = builder(getTmpDir(), this.wireType)
                .build()) {

            File name2 = getTmpDir();
            try (ChronicleQueue chronicle = builder(name2, this.wireType)
                    .build()) {

                ExcerptAppender appender = chronicle.acquireAppender();

                MapWrapper myMap = new MapWrapper();
                myMap.map.put("hello", 1.2);

                appender.writeDocument(w -> w.write().object(myMap));

                ExcerptTailer tailer = chronicle.createTailer(named ? "named" : null);

                try (DocumentContext documentContext = tailer.readingDocument()) {
                    MapWrapper object = documentContext.wire().read().object(MapWrapper.class);
                    assertEquals(1.2, object.map.get("hello"), 0.0);
                }
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
            // System.out.println(chronicle.dump());
            assertEquals(nextIndexToWrite,
                    appender.lastIndexAppended());
        }
    }

    @Test
    public void testAppendedSkipToEndMultiThreaded() throws InterruptedException {
        // some text to simulate load.
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 5; i++) sb.append(UUID.randomUUID());
        String text = sb.toString();

        try (ChronicleQueue q = builder(getTmpDir(), this.wireType)
                .rollCycle(TEST_SECONDLY)
                .build()) {

            System.err.println(q.file());
            final ThreadLocal<ExcerptAppender> tl = ThreadLocal.withInitial(q::acquireAppender);

            int size = 50_000;
            int threadCount = 8;
            int sizePerThread = size / threadCount;
            CountDownLatch latch = new CountDownLatch(threadCount);

            for (int j = 0; j < threadCount; j++) {
                new Thread(() -> {
                    for (int i = 0; i < sizePerThread; i++)
                        writeTestDocument(tl, text);
                    latch.countDown();
                }).start();
            }

            latch.await();

            ExcerptTailer tailer = q.createTailer(named ? "named" : null);
            for (int i = 0; i < size; i++) {
                try (DocumentContext dc = tailer.readingDocument(false)) {
                    long index = dc.index();
                    long actual = dc.wire().read("key").int64();

                    assertEquals(toTextIndex(q, index), toTextIndex(q, actual));
                }
            }
        }
    }

    @NotNull
    private String toTextIndex(ChronicleQueue q, long index) {
        return Long.toHexString(q.rollCycle().toCycle(index)) + "_" + Long.toHexString(q.rollCycle().toSequenceNumber(index));
    }

    /**
     * if one appender if much further ahead than the other, then the new append should jump straight to the end rather than attempting to write a
     * positions that are already occupied
     */
    @Test
    public void testAppendedSkipToEnd() {

        try (ChronicleQueue q = builder(getTmpDir(), this.wireType)
                .build()) {

            ExcerptAppender appender = q.acquireAppender();
            ExcerptAppender appender2 = q.acquireAppender();
            int indexCount = 100;

            for (int i = 0; i < indexCount; i++) {
                try (DocumentContext dc = appender.writingDocument()) {
                    dc.wire().write("key").text("some more " + 1);
                }
                assertEquals(i, q.rollCycle().toSequenceNumber(appender.lastIndexAppended()));
            }

            try (DocumentContext dc = appender2.writingDocument()) {
                dc.wire().write("key").text("some data " + indexCount);
            }
            assertEquals(indexCount, q.rollCycle().toSequenceNumber(appender.lastIndexAppended()));
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
        AbstractCloseable.assertCloseablesClosed();
        clock.addAndGet(1100);

        // this will write an EOF
        try (ChronicleQueue q = builder(dir, wireType)
                .rollCycle(TEST_SECONDLY)
                .timeProvider(clock::get)
                .build()) {

            ExcerptTailer tailer = q.createTailer(named ? "named" : null);

            assertEquals("first", tailer.readText());
            assertNull(tailer.readText());

        }
        AbstractCloseable.assertCloseablesClosed();

        try (ChronicleQueue q = builder(dir, wireType)
                .rollCycle(TEST_SECONDLY)
                .timeProvider(clock::get)
                .build()) {

            ExcerptTailer tailer = q.createTailer(named ? "named" : null).toEnd();

            try (DocumentContext documentContext = tailer.readingDocument()) {
                assertFalse(documentContext.isPresent());
            }

            try (DocumentContext documentContext = tailer.readingDocument()) {
                assertFalse(documentContext.isPresent());
            }
        }
        AbstractCloseable.assertCloseablesClosed();

        clock.addAndGet(50L);

        try (ChronicleQueue q = builder(dir, wireType)
                .rollCycle(TEST_SECONDLY)
                .timeProvider(clock::get)
                .build()) {

            ExcerptTailer excerptTailerBeforeAppend = q.createTailer(named ? "named" : null).toEnd();
            q.acquireAppender().writeText("more text");
            ExcerptTailer excerptTailerAfterAppend = q.createTailer(named ? "named" : null).toEnd();
            q.acquireAppender().writeText("even more text");

            assertEquals("more text", excerptTailerBeforeAppend.readText());
            assertEquals("even more text", excerptTailerAfterAppend.readText());
            assertEquals("even more text", excerptTailerBeforeAppend.readText());
        }
        AbstractCloseable.assertCloseablesClosed();

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

                final ThreadLocal<ExcerptAppender> tl = ThreadLocal.withInitial(q::acquireAppender);
                final ThreadLocal<ExcerptTailer> tlt = ThreadLocal.withInitial(q::createTailer);

                int size = 20_000_000;

                for (int j = 0; j < size; j++)
                    executor.execute(() -> doSomething(tl, tlt, text));

                executor.shutdown();
                if (!executor.awaitTermination(10_000, TimeUnit.SECONDS))
                    executor.shutdownNow();

                // System.out.println(". " + i);
                Jvm.pause(1000);
            }
        }
    }

    @Test
    public void testTailerWhenCyclesWhereSkippedOnWrite() {
        SetTimeProvider timeProvider = new SetTimeProvider();

        try (final ChronicleQueue queue = binary(getTmpDir())
                .rollCycle(RollCycles.TEST_SECONDLY).timeProvider(timeProvider)
                .build();
             final ExcerptTailer tailer = queue.createTailer(named ? "named" : null)) {
            try (final ExcerptAppender appender = queue.acquireAppender()) {

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

                for (String expected : stringsToPut) {
                    try (DocumentContext readingContext = tailer.readingDocument()) {
                        if (!readingContext.isPresent())
                            fail();
                        String text = readingContext.wire().read().text();
                        assertEquals(expected, text);

                    }
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
            assertEquals(dc.index(), dc.wire().read("key").int64());
            assertEquals(text, dc.wire().read("text").text());
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
                .timeProvider(new SetTimeProvider("2020/10/19T01:01:01"))
                .build();
             ExcerptAppender syncA = syncQ.acquireAppender();
             ExcerptAppender syncB = syncQ.acquireAppender();
             ExcerptAppender syncC = syncQ.acquireAppender()) {
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
            assertEquals(expected, syncQ.dump().replaceAll("(?m)^#.+$\\n", ""));
        }
    }

    @NotNull
    protected String expectedMultipleAppenders() {
        if (wireType == WireType.BINARY || wireType == WireType.BINARY_LIGHT || wireType == WireType.COMPRESSED_BINARY)
            return "" +
                    "--- !!meta-data #binary\n" +
                    "header: !STStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  metadata: !SCQMeta {\n" +
                    "    roll: !SCQSRoll { length: 86400000, format: yyyyMMdd'T1', epoch: 0 },\n" +
                    "    deltaCheckpointInterval: 64,\n" +
                    "    sourceId: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "--- !!data #binary\n" +
                    "listing.highestCycle: 18554\n" +
                    "--- !!data #binary\n" +
                    "listing.lowestCycle: 18554\n" +
                    "--- !!data #binary\n" +
                    "listing.modCount: 3\n" +
                    queueLockForTestReentrant() +
                    "--- !!data #binary\n" +
                    "chronicle.write.lock: -9223372036854775808\n" +
                    "--- !!data #binary\n" +
                    "chronicle.append.lock: -9223372036854775808\n" +
                    "--- !!data #binary\n" +
                    "chronicle.lastIndexReplicated: -1\n" +
                    "--- !!data #binary\n" +
                    "chronicle.lastAcknowledgedIndexReplicated: -1\n" +
                    "...\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  writePosition: [\n" +
                    "    512,\n" +
                    "    2199023255557\n" +
                    "  ],\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 200,\n" +
                    "    lastIndex: 6\n" +
                    "  },\n" +
                    "  dataFormat: 1\n" +
                    "}\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  304,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 6\n" +
                    "  400,\n" +
                    "  416,\n" +
                    "  448,\n" +
                    "  464,\n" +
                    "  496,\n" +
                    "  512,\n" +
                    "  0, 0\n" +
                    "]\n" +
                    "--- !!data #binary\n" +
                    "hello A0\n" +
                    "--- !!data #binary\n" +
                    "hello B0\n" +
                    "--- !!meta-data #binary\n" +
                    "some meta 0\n" +
                    "--- !!data #binary\n" +
                    "hello A1\n" +
                    "--- !!data #binary\n" +
                    "hello B1\n" +
                    "--- !!meta-data #binary\n" +
                    "some meta 1\n" +
                    "--- !!data #binary\n" +
                    "hello A2\n" +
                    "--- !!data #binary\n" +
                    "hello B2\n" +
                    "--- !!meta-data #binary\n" +
                    "some meta 2\n" +
                    "...\n";

        throw new IllegalStateException("unknown wiretype=" + wireType);
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

        // this will write an EOF
        try (ChronicleQueue q = builder(dir, wireType)
                .rollCycle(TEST_SECONDLY)
                .timeProvider(clock::get)
                .build();

             ExcerptTailer tailer = q.createTailer(named ? "named" : null)) {

            assertEquals("first", tailer.readText());
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
    public void testReadingWritingWhenNextCycleIsInSequence() {
        SetTimeProvider timeProvider = new SetTimeProvider();

        final File dir = getTmpDir();
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
                .rollCycle(rollCycle).timeProvider(timeProvider).build();
             ExcerptTailer tailer = queue.createTailer(named ? "named" : null)) {
            assertEquals("first message", tailer.readText());
            assertEquals("second message", tailer.readText());
        }
    }

    @Test
    public void testReadingWritingWhenCycleIsSkipped() {

        SetTimeProvider timeProvider = new SetTimeProvider();

        final File dir = getTmpDir();
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
                .rollCycle(rollCycle).timeProvider(timeProvider).build();
             ExcerptTailer tailer = queue.createTailer(named ? "named" : null)) {
            assertEquals("first message", tailer.readText());
            assertEquals("second message", tailer.readText());
        }
    }

    @Test
    public void testReadingWritingWhenCycleIsSkippedBackwards() {
        final SetTimeProvider timeProvider = new SetTimeProvider();
        long time = System.currentTimeMillis();
        timeProvider.currentTimeMillis(time);

        final File dir = getTmpDir();
        final RollCycles rollCycle = RollCycles.TEST_SECONDLY;

        // write first message
        try (ChronicleQueue queue = binary(dir).rollCycle(rollCycle).timeProvider(timeProvider).build()) {
            queue.acquireAppender().writeText("first message");
        }

        timeProvider.advanceMillis(2100);

        // write second message
        try (ChronicleQueue queue = binary(dir).rollCycle(rollCycle).timeProvider(timeProvider).build()) {
            queue.acquireAppender().writeText("second message");
        }

        // read both messages
        try (ChronicleQueue queue = binary(dir).rollCycle(rollCycle).timeProvider(timeProvider).build();
             ExcerptTailer tailer = queue.createTailer(named ? "named" : null)) {
            ExcerptTailer excerptTailer = tailer.direction(TailerDirection.BACKWARD).toEnd();
            assertEquals("second message", excerptTailer.readText());
            assertEquals("first message", excerptTailer.readText());
        }
    }

    @Test
    public void testReadWritingWithTimeProvider() {
        final File dir = getTmpDir();

        long time = System.currentTimeMillis();

        SetTimeProvider timeProvider = new SetTimeProvider();
        timeProvider.currentTimeMillis(time);
        try (ChronicleQueue q1 = binary(dir)
                .timeProvider(timeProvider)
                .build()) {

            try (ChronicleQueue q2 = binary(dir)
                    .timeProvider(timeProvider)
                    .build();

                 final ExcerptAppender appender2 = q2.acquireAppender();
                 final ExcerptTailer tailer1 = q1.createTailer(named ? "named" : null);
                 final ExcerptTailer tailer2 = q2.createTailer(named ? "named" : null)) {

                try (final DocumentContext dc = appender2.writingDocument()) {
                    dc.wire().write().text("some data");
                }

                try (DocumentContext dc = tailer2.readingDocument()) {
                    assertTrue(dc.isPresent());
                }

                assertEquals(q1.file(), q2.file());
                // this is required for queue to re-request last/first cycle
                timeProvider.advanceMillis(1);

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
    public void testCountExceptsBetweenCycles() {
        SetTimeProvider timeProvider = new SetTimeProvider();

        try (final RollingChronicleQueue queue = binary(getTmpDir())
                .rollCycle(RollCycles.TEST_SECONDLY)
                .timeProvider(timeProvider)
                .build();
             final ExcerptAppender appender = queue.acquireAppender()) {

            long[] indexs = new long[10];
            for (int i = 0; i < indexs.length; i++) {
                // System.out.println(".");
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
                    // System.out.println("lower=" + lower + ",upper=" + upper);
                    assertEquals(upper - lower, queue.countExcerpts(indexs[lower],
                            indexs[upper]));
                }
            }

            // check the base line of the test below
            assertEquals(6, queue.countExcerpts(indexs[0], indexs[6]));

            /// check for the case when the last index has a sequence number of -1
            assertEquals(0, queue.rollCycle().toSequenceNumber(indexs[6]));
            assertEquals(5, queue.countExcerpts(indexs[0],
                    indexs[6] - 1));

            /// check for the case when the first index has a sequence number of -1
            assertEquals(7, queue.countExcerpts(indexs[0] - 1,
                    indexs[6]));
        }
    }

    @Test
    public void testLongLivingTailerAppenderReAcquiredEachSecond() {
        SetTimeProvider timeProvider = new SetTimeProvider();
        final File dir = getTmpDir();
        final RollCycles rollCycle = TEST4_SECONDLY;

        try (ChronicleQueue queuet = binary(dir)
                .rollCycle(rollCycle)
                .testBlockSize()
                .timeProvider(timeProvider)
                .build();
             final ExcerptTailer tailer = queuet.createTailer(named ? "named" : null)) {

            // The look up of the first and last cycle is cached at this point and won't be checked again for 1 millisecond to reduce overhead.
            Jvm.pause(1);

            // write first message
            try (ChronicleQueue queue =
                         binary(dir)
                                 .rollCycle(rollCycle)
                                 .testBlockSize()
                                 .timeProvider(timeProvider)
                                 .build()) {

                for (int i = 0; i < 5; i++) {
                    Jvm.pause(1);

                    final ExcerptAppender appender = queue.acquireAppender();
                    timeProvider.advanceMillis(1100);
                    try (final DocumentContext dc = appender.writingDocument()) {
                        dc.wire().write("some").int32(i);
                    }

                    try (final DocumentContext dc = tailer.readingDocument()) {
                        if (!dc.isPresent())
                            System.out.println(queue.dump());

                        assertTrue(dc.isPresent());
                        assertEquals(i, dc.wire().read("some").int32());
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

    @Test
    public void testFromSizePrefixedBlobs() {

        try (final ChronicleQueue queue = binary(getTmpDir())
                .build()) {

            try (DocumentContext dc = queue.acquireAppender().writingDocument()) {
                dc.wire().write("some").text("data");
            }
            String s = null;

            DocumentContext dc0;
            try (DocumentContext dc = queue.createTailer(named ? "named" : null).readingDocument()) {
                s = Wires.fromSizePrefixedBlobs(dc);
                assertTrue(s.contains("some: data"));
                dc0 = dc;
            }

            String out = Wires.fromSizePrefixedBlobs(dc0);
            assertEquals(s, out);

        }
    }

    @Test
    public void tailerRollBackTest() {
        final File source = getTmpDir();
        try (final ChronicleQueue q = binary(source).build()) {

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
        final File source = getTmpDir();
        final File target = getTmpDir();
        {

            try (final ChronicleQueue q =
                         binary(source)
                                 .build();
                 ExcerptAppender excerptAppender = q.acquireAppender()) {

                excerptAppender.writeMessage("one", 1);
                excerptAppender.writeMessage("two", 2);
                excerptAppender.writeMessage("three", 3);
                excerptAppender.writeMessage("four", 4);
            }
        }
        {
            try (final ChronicleQueue s = binary(source).build();
                 final ChronicleQueue t = binary(target).build();
                 ExcerptTailer sourceTailer = s.createTailer(named ? "named" : null);
                 ExcerptAppender appender = t.acquireAppender()) {

                for (; ; ) {
                    try (DocumentContext rdc = sourceTailer.readingDocument()) {
                        if (!rdc.isPresent())
                            break;

                        try (DocumentContext wdc = appender.writingDocument()) {
                            final Bytes<?> bytes = rdc.wire().bytes();
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

        try (final ChronicleQueue queue = binary(getTmpDir())
                .rollCycle(DAILY).build()) {

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
                        dc.wire().write("value").int64(v);
                    } else {
                        dc.wire().write("value").int64(0);
                    }
                }
            }

            try (ExcerptTailer tailer = queue.createTailer(named ? "named" : null)) {

                assertTrue(tailer.moveToIndex(startIndex));

                tailer.direction(TailerDirection.FORWARD);
                assertEquals(0, action(tailer, queue.rollCycle()));
                assertEquals(1, action(tailer, queue.rollCycle()));

                tailer.direction(TailerDirection.BACKWARD);
                assertEquals(2, action(tailer, queue.rollCycle()));
                assertEquals(1, action(tailer, queue.rollCycle()));

                tailer.direction(TailerDirection.FORWARD);
                assertEquals(0, action(tailer, queue.rollCycle()));
                assertEquals(1, action(tailer, queue.rollCycle()));
            }
        }
    }

    @Test
    public void testExistingRollCycleIsMaintained() {
        expectException("Overriding roll cycle from ");
        expectException("Overriding roll length from ");

        RollCycles[] values = values();
        for (int i = 0; i < values.length - 1; i++) {
            final File tmpDir = getTmpDir();

            try (final ChronicleQueue queue = binary(tmpDir)
                    .rollCycle(values[i]).build()) {
                queue.acquireAppender().writeText("hello world");
            }

            try (final ChronicleQueue queue = binary(tmpDir)
                    .rollCycle(values[i + 1]).build()) {
                assertEquals(values[i], queue.rollCycle());
            }
        }
    }

    private long action(@NotNull final ExcerptTailer tailer1, @NotNull final RollCycle rollCycle) {
        try (final DocumentContext dc = tailer1.readingDocument()) {
            return dc.wire().read("value").int64();
        } finally {
            rollCycle.toSequenceNumber(tailer1.index());
        }
    }

    @Test
    public void checkReferenceCountingAndCheckFileDeletion() {

        MappedFile mappedFile;

        try (ChronicleQueue queue =
                     binary(getTmpDir())
                             .rollCycle(RollCycles.TEST_SECONDLY)
                             .build()) {
            ExcerptAppender appender = queue.acquireAppender();

            try (DocumentContext documentContext1 = appender.writingDocument()) {
                documentContext1.wire().write().text("some text");
            }

            try (DocumentContext documentContext = queue.createTailer(named ? "named" : null).readingDocument()) {
                mappedFile = toMappedFile(documentContext);
                assertEquals("some text", documentContext.wire().read().text());
            }
        }

        waitFor(mappedFile::isClosed, "mappedFile is not closed");

        if (OS.isWindows()) {
            System.err.println("#460 Cannot test delete after close on windows");
            return;
        }
        // this used to fail on windows
        assertTrue(mappedFile.file().delete());

    }

    @Test
    public void checkReferenceCountingWhenRollingAndCheckFileDeletion() {
        SetTimeProvider timeProvider = new SetTimeProvider();

        @SuppressWarnings("unused")
        MappedFile mappedFile1, mappedFile2, mappedFile3, mappedFile4;

        try (ChronicleQueue queue =
                     binary(getTmpDir())
                             .rollCycle(RollCycles.TEST_SECONDLY)
                             .timeProvider(timeProvider)
                             .build();
             ExcerptAppender appender = queue.acquireAppender()) {

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write().text("some text");
                mappedFile1 = toMappedFile(dc);
            }
            timeProvider.advanceMillis(1100);
            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write().text("some more text");
                mappedFile2 = toMappedFile(dc);
            }

            try (ExcerptTailer tailer = queue.createTailer(named ? "named" : null)) {
                try (DocumentContext documentContext = tailer.readingDocument()) {
                    mappedFile3 = toMappedFile(documentContext);
                    assertEquals("some text", documentContext.wire().read().text());

                }

                try (DocumentContext documentContext = tailer.readingDocument()) {
                    mappedFile4 = toMappedFile(documentContext);
                    assertEquals("some more text", documentContext.wire().read().text());

                }
            }
        }

        waitFor(mappedFile1::isClosed, "mappedFile1 is not closed");
        waitFor(mappedFile2::isClosed, "mappedFile2 is not closed");

        if (OS.isWindows()) {
            // TODO: retest
            System.err.println("#460 Cannot test delete after close on windows");
            return;
        }
        // this used to fail on windows
        assertTrue(mappedFile1.file().delete());
        assertTrue(mappedFile2.file().delete());
    }

    @Test(timeout = 10_000)
    public void testWritingDocumentIsAtomic() {

        final int threadCount = 8;
        final ExecutorService executorService = Executors.newFixedThreadPool(threadCount,
                new NamedThreadFactory("test"));
        // remove change of cycle roll in test, cross-cycle atomicity is covered elsewhere
        final AtomicLong fixedClock = new AtomicLong(System.currentTimeMillis());
        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(getTmpDir())
                .rollCycle(RollCycles.TEST_SECONDLY)
                .timeoutMS(3_000)
                .timeProvider(fixedClock::get)
                .testBlockSize()
                .build()) {
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
            ExcerptTailer tailer = queue.createTailer(named ? "named" : null);
            for (int expected = 0; expected < totalIterations; expected++) {
                for (; ; ) {
                    if (System.currentTimeMillis() > timeout)
                        fail("Timed out, having read " + expected + " documents of " + totalIterations);
                    try (DocumentContext dc = tailer.readingDocument()) {
                        if (!dc.isPresent()) {
                            Thread.yield();
                            continue;
                        }

                        long justRead = dc.wire().read("some key").int64();
                        assertEquals(expected, justRead);
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
    public void shouldBeAbleToLoadQueueFromReadOnlyFiles() throws IOException {
        if (OS.isWindows()) {
            System.err.println("#460 Cannot test read only mode on windows");
            return;
        }
        assumeFalse(named);

        final File queueDir = getTmpDir();
        try (final ChronicleQueue queue = builder(queueDir, wireType).
                testBlockSize().build()) {
            queue.acquireAppender().writeDocument("foo", (v, t) -> {
                v.text(t);
            });
        }

        try (Stream<Path> list = Files.list(queueDir.toPath())) {
            list.forEach(p -> assertTrue(p.toFile().setReadOnly()));
        }

        try (final ChronicleQueue queue = builder(queueDir, wireType).
                readOnly(true).
                testBlockSize().build()) {
            assertTrue(queue.createTailer(named ? "named" : null).readingDocument().isPresent());
        }
    }

    @Test
    public void shouldCreateQueueInCurrentDirectory() {
        if (OS.isWindows()) {
            System.err.println("#460 Cannot test delete after close on windows");
            return;
        }

        try (final ChronicleQueue ignored =
                     builder(new File(""), wireType).
                             testBlockSize().build()) {

        }

        assertTrue(new File(QUEUE_METADATA_FILE).delete());
    }

    @NotNull
    protected SingleChronicleQueueBuilder builder(@NotNull File file, @NotNull WireType wireType) {
        return SingleChronicleQueueBuilder.builder(file, wireType).rollCycle(RollCycles.TEST4_DAILY).testBlockSize();
    }

    @Test
    public void testTailerSnappingRollWithNewAppender() throws InterruptedException, ExecutionException, TimeoutException {
        SetTimeProvider timeProvider = new SetTimeProvider();
        timeProvider.currentTimeMillis(System.currentTimeMillis() - 2_000);
        final File dir = getTmpDir();
        final RollCycles rollCycle = RollCycles.TEST_SECONDLY;

        // write first message
        try (ChronicleQueue queue =
                     binary(dir)
                             .rollCycle(rollCycle)
                             .timeProvider(timeProvider)
                             .build()) {
            ExcerptAppender excerptAppender = queue.acquireAppender();
            excerptAppender.writeText("someText");

            ExecutorService executorService = Executors.newFixedThreadPool(2,
                    new NamedThreadFactory("test"));

            Future<?> f1 = executorService.submit(() -> {

                try (ChronicleQueue queue2 = binary(dir)
                        .rollCycle(rollCycle).timeProvider(timeProvider).build()) {
                    queue2.acquireAppender().writeText("someText more");
                }
                timeProvider.advanceMillis(1100);
                try (ChronicleQueue queue2 = binary(dir)
                        .rollCycle(rollCycle).timeProvider(timeProvider).build()) {
                    queue2.acquireAppender().writeText("someText more");
                }
            });

            Future<?> f2 = executorService.submit(() -> {

                // write second message
                try (ChronicleQueue queue2 = binary(dir)
                        .rollCycle(rollCycle).timeProvider(timeProvider).build()) {

                    for (int i = 0; i < 5; i++) {
                        queue2.acquireAppender().writeText("someText more");
                        timeProvider.advanceMillis(400);
                    }
                }
            });

            f1.get(10, TimeUnit.SECONDS);
            // System.out.println(queue.dump().replaceAll("(?m)^#.+$\\n", ""));
            f2.get(10, TimeUnit.SECONDS);

            executorService.shutdownNow();
        }
    }

    @NotNull
    protected SingleChronicleQueueBuilder builderWithAppendListener(@NotNull File file, @NotNull WireType wireType) {
        appenderListenerDump.clear();
        return SingleChronicleQueueBuilder.builder(file, wireType)
                .rollCycle(RollCycles.TEST4_DAILY)
                .timeProvider(new SetTimeProvider("2021/11/17T12:34:56").advanceMillis(1000))
                .appenderListener((w, idx) -> {
                    appenderListenerDump.append("idx: ").append(Long.toHexString(idx)).append("\n");
                    w.bytes().readSkip(-4);
                    appenderListenerDump.append(Wires.fromSizePrefixedBlobs(w)).append("\n");
                })
                .testBlockSize();
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

    @Test
    public void writeBytesAndIndexFiveTimesWithOverwriteTest() {
        try (final ChronicleQueue sourceQueue =
                     builder(getTmpDir(), wireType).
                             testBlockSize().build()) {

            for (int i = 0; i < 5; i++) {
                ExcerptAppender excerptAppender = sourceQueue.acquireAppender();
                try (DocumentContext dc = excerptAppender.writingDocument()) {
                    dc.wire().write("hello").text("world" + i);
                }
            }

            try (ExcerptTailer tailer = sourceQueue.createTailer(named ? "named" : null);
                 ChronicleQueue queue =
                         builder(getTmpDir(), wireType).testBlockSize().build()) {

                ExcerptAppender appender0 = queue.acquireAppender();

                assumeTrue(appender0 instanceof InternalAppender);
                InternalAppender appender = (InternalAppender) appender0;
                assumeTrue(appender instanceof StoreAppender);

                List<BytesWithIndex> bytesWithIndies = new ArrayList<>();
                try {
                    for (int i = 0; i < 5; i++) {
                        bytesWithIndies.add(bytes(tailer));
                    }

                    // ... and try and overwrite starting at beginning
                    // TODO: if you step in here it looks like it is overwriting
                    // and DOES NOT output debug log "Trying to overwrite index..."
                    for (int i = 0; i < 4; i++) {
                        BytesWithIndex b = bytesWithIndies.get(i);
                        appender.writeBytes(b.index, b.bytes);
                    }

                    // this will output debug log "Trying to overwrite index..." as expected
                    for (int i = 0; i < 4; i++) {
                        BytesWithIndex b = bytesWithIndies.get(i);
                        appender.writeBytes(b.index, b.bytes);
                    }

                    BytesWithIndex b = bytesWithIndies.get(4);
                    appender.writeBytes(b.index, b.bytes);

                    ((StoreAppender) appender).checkWritePositionHeaderNumber();
                    appender0.writeText("goodbye");
                } finally {
                    closeQuietly(bytesWithIndies);
                }

                String dump = queue.dump().replaceAll("(?m)^#.+$\\n", "");
                assertTrue(dump, dump.contains(
                        "--- !!data #binary\n" +
                                "hello: world0\n" +
                                "--- !!data #binary\n" +
                                "hello: world1\n" +
                                "--- !!data #binary\n" +
                                "hello: world2\n" +
                                "--- !!data #binary\n" +
                                "hello: world3\n" +
                                "--- !!data #binary\n" +
                                "hello: world4\n" +
                                "--- !!data #binary\n" +
                                "goodbye\n"));

            }
        }
    }

    @Test
    public void writeBytesAndIndexFiveTimesTest() {
        try (final ChronicleQueue sourceQueue =
                     builder(getTmpDir(), wireType).
                             testBlockSize().build()) {

            for (int i = 0; i < 5; i++) {
                ExcerptAppender excerptAppender = sourceQueue.acquireAppender();
                try (DocumentContext dc = excerptAppender.writingDocument()) {
                    dc.wire().write("hello").text("world" + i);
                }
            }

            String before = sourceQueue.dump().replaceAll("(?m)^#.+$\\n", "");
            try (ExcerptTailer tailer = sourceQueue.createTailer(named ? "named" : null);
                 ChronicleQueue queue =
                         builder(getTmpDir(), wireType).testBlockSize().build()) {

                ExcerptAppender appender = queue.acquireAppender();

                if (!(appender instanceof StoreAppender))
                    return;

                for (int i = 0; i < 5; i++) {
                    try (final BytesWithIndex b = bytes(tailer)) {
                        ((InternalAppender) appender).writeBytes(b.index, b.bytes);
                    }
                }

                String dump = queue.dump().replaceAll("(?m)^#.+$\\n", "");
                assertEquals(before, dump);
                assertTrue(dump, dump.contains(
                        "--- !!data #binary\n" +
                                "hello: world0\n" +
                                "--- !!data #binary\n" +
                                "hello: world1\n" +
                                "--- !!data #binary\n" +
                                "hello: world2\n" +
                                "--- !!data #binary\n" +
                                "hello: world3\n" +
                                "--- !!data #binary\n" +
                                "hello: world4"));
            }
        }
    }

    @Test
    public void rollbackTest() {

        File file = getTmpDir();
        try (final ChronicleQueue sourceQueue =
                     builder(file, wireType).
                             testBlockSize().build();
             ExcerptAppender excerptAppender = sourceQueue.acquireAppender()) {
            try (DocumentContext dc = excerptAppender.writingDocument()) {
                dc.wire().write("hello").text("world1");
            }
            try (DocumentContext dc = excerptAppender.writingDocument()) {
                dc.wire().write("hello2").text("world2");
            }
            try (DocumentContext dc = excerptAppender.writingDocument()) {
                dc.wire().write("hello3").text("world3");
            }
        }
        try (final ChronicleQueue queue =
                     builder(file, wireType).testBlockSize().build();
             ExcerptTailer tailer1 = queue.createTailer(named ? "named" : null)) {

            StringBuilder sb = Wires.acquireStringBuilder();
            try (DocumentContext documentContext = tailer1.readingDocument()) {
                documentContext.wire().readEventName(sb);
                assertEquals("hello", sb.toString());
                documentContext.rollbackOnClose();

            }

            try (DocumentContext documentContext = tailer1.readingDocument()) {
                documentContext.wire().readEventName(sb);
                assertEquals("hello", sb.toString());
            }

            try (DocumentContext documentContext = tailer1.readingDocument()) {
                documentContext.wire().readEventName(sb);
                documentContext.rollbackOnClose();
                assertEquals("hello2", sb.toString());

            }

            try (DocumentContext documentContext = tailer1.readingDocument()) {
                Bytes<?> bytes = documentContext.wire().bytes();
                long rp = bytes.readPosition();
                long wp = bytes.writePosition();
                long wl = bytes.writeLimit();

                try {
                    documentContext.wire().readEventName(sb);
                    assertEquals("hello2", sb.toString());
                    documentContext.rollbackOnClose();
                } finally {
                    bytes.readPosition(rp).writePosition(wp).writeLimit(wl);
                }
            }

            try (DocumentContext documentContext = tailer1.readingDocument()) {
                documentContext.wire().readEventName(sb);
                assertEquals("hello2", sb.toString());
            }
            try (DocumentContext documentContext = tailer1.readingDocument()) {
                documentContext.wire().readEventName(sb);
                assertEquals("hello3", sb.toString());
                documentContext.rollbackOnClose();
            }
            try (DocumentContext documentContext = tailer1.readingDocument()) {
                assertTrue(documentContext.isPresent());
                documentContext.wire().readEventName(sb);
                assertEquals("hello3", sb.toString());
            }
            try (DocumentContext documentContext = tailer1.readingDocument()) {
                assertFalse(documentContext.isPresent());
                documentContext.rollbackOnClose();
            }
            try (DocumentContext documentContext = tailer1.readingDocument()) {
                assertFalse(documentContext.isPresent());
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

    @Ignore("TODO FIX https://github.com/OpenHFT/Chronicle-Core/issues/121")
    @Test
    public void mappedSegmentsShouldBeUnmappedAsCycleRolls() throws IOException, InterruptedException {

        Assume.assumeTrue("this test is slow and does not depend on wire type", wireType == WireType.BINARY);

        long now = System.currentTimeMillis();
        long ONE_HOUR_IN_MILLIS = 60 * 60 * 1000;
        long ONE_DAY_IN_MILLIS = ONE_HOUR_IN_MILLIS * 24;
        long midnight = now - (now % ONE_DAY_IN_MILLIS);
        AtomicLong clock = new AtomicLong(now);

        StringBuilder builder = new StringBuilder();
        boolean passed = doMappedSegmentUnmappedRollTest(clock, builder);
        passed = passed && doMappedSegmentUnmappedRollTest(setTime(clock, midnight), builder);
        for (int i = 1; i < 3; i += 1)
            passed = passed && doMappedSegmentUnmappedRollTest(setTime(clock, midnight + (i * ONE_HOUR_IN_MILLIS)), builder);

        if (!passed) {
            fail(builder.toString());
        }
    }

    private AtomicLong setTime(AtomicLong clock, long newValue) {
        clock.set(newValue);
        return clock;
    }

    private boolean doMappedSegmentUnmappedRollTest(AtomicLong clock, StringBuilder builder) throws IOException, InterruptedException {
        String time = Instant.ofEpochMilli(clock.get()).toString();

        final Random random = new Random(0xDEADBEEF);
        final File queueFolder = getTmpDir();
        try (final ChronicleQueue queue = ChronicleQueue.singleBuilder(queueFolder).
                timeProvider(clock::get).
                testBlockSize().rollCycle(RollCycles.HOURLY).
                build();
             ExcerptAppender appender = queue.acquireAppender()) {
            for (int i = 0; i < 20_000; i++) {
                final int batchSize = random.nextInt(10);
                appender.writeDocument(batchSize, ValueOut::int64);
                final byte payload = (byte) random.nextInt();
                for (int j = 0; j < batchSize; j++) {
                    appender.writeDocument(payload, ValueOut::int8);
                }
                if (random.nextDouble() > 0.995) {
                    clock.addAndGet(TimeUnit.MINUTES.toMillis(37L));
                    // this give the reference processor a chance to run
                    Jvm.pause(30);
                }
            }

            boolean passed = true;
            if (OS.isLinux()) {
                List<String> openFiles = getMappedQueueFileCount();
                int filesOpen = openFiles.size();
                if (filesOpen >= 50) {
                    passed = false;
                    builder.append(String.format("Test for time %s failed: Too many mapped files: %d%n", time, filesOpen));
                    builder.append("Open files:").append("\n");
                    openFiles.stream().map(s -> s + "\n").forEach(builder::append);
                }
            }

            try (Stream<Path> list = Files.list(queueFolder.toPath())) {
                long fileCount = list.filter(p -> p.toString().endsWith(SUFFIX)).count();
                if (fileCount <= 10L) {
                    passed = false;
                    builder.append(String.format("Test for time %s failed: Too many mapped files: %d%n", time, fileCount));
                }

                if (passed) {
                    builder.append(String.format("Test for time %s passed!%n", time));
                }

                return passed;
            }
        }
    }

    /**
     * relates to https://github.com/OpenHFT/Chronicle-Queue/issues/699
     */
    @Test
    public void testReadUsingReadOnly() {
        assumeFalse("Read-only mode is not supported on Windows", OS.isWindows());
        assumeFalse(named);

        File tmpDir = getTmpDir();
        String expected = "hello world";
        try (ChronicleQueue out = SingleChronicleQueueBuilder
                .binary(tmpDir)
                .build()) {
            try (DocumentContext dc = out.acquireAppender().writingDocument()) {
                dc.wire().getValueOut().text(expected);
            }
        }

        try (ChronicleQueue out = SingleChronicleQueueBuilder
                .binary(tmpDir)
                .readOnly(true)
                .build()) {
            StringBuilder sb = new StringBuilder();
            try (DocumentContext dc = out.createTailer().readingDocument()) {
                dc.wire().getValueIn().text(sb);
            }

            Assert.assertEquals(expected, sb.toString());
        }
    }

    @Test
    public void lastIndexShouldReturnLastIndexForPopulatedQueue() {
        File tmpDir = getTmpDir();
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.single(tmpDir).wireType(wireType).build()) {
            long actualLastIndex;
            try (ExcerptAppender appender = queue.acquireAppender()) {
                appender.writeText("Hello!");
                actualLastIndex = appender.lastIndexAppended();
            }
            assertEquals(actualLastIndex, queue.lastIndex());
        }
    }

    @Test
    public void lastIndexShouldReturnNegativeOneForEmptyQueue() {
        File tmpDir = getTmpDir();
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.single(tmpDir).wireType(wireType).build()) {
            assertEquals(-1, queue.lastIndex());
        }
    }

    @Test
    public void lastIndexShouldReturnNegativeOneForMetadataOnlyQueue() {
        File tmpDir = getTmpDir();
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.single(tmpDir).wireType(wireType).build()) {
            try (ExcerptAppender appender = queue.acquireAppender()) {
                try (DocumentContext documentContext = appender.writingDocument(true)) {
                    documentContext.wire().write().text("Hello!");
                }
            }
            assertEquals(-1, queue.lastIndex());
        }
    }

    @Test
    public void lastIndexShouldReturnNegativeOneForEmptyPretouchedQueue() {
        File tmpDir = getTmpDir();
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.single(tmpDir).wireType(wireType).build()) {
            try (ExcerptAppender appender = queue.acquireAppender()) {
                appender.pretouch();
            }
            assertEquals(-1, queue.lastIndex());
        }
    }

    @Test
    public void shouldWaitForConditionWhenAcquiringAppender() throws TimeoutException {
        File tmpDir = getTmpDir();
        AtomicBoolean gotAppender = new AtomicBoolean(false);
        ReentrantLock acquireAppenderLock = new ReentrantLock();
        final Condition createAppenderCondition = acquireAppenderLock.newCondition();
        try (final SingleChronicleQueue queue = SingleChronicleQueueBuilder.single(tmpDir)
                .wireType(wireType)
                .createAppenderConditionCreator(q -> createAppenderCondition)
                .build()) {
            new Thread(() -> {
                acquireAppenderLock.lock();
                queue.acquireAppender();
                gotAppender.set(true);
            }).start();

            // Assert acquireAppender is blocked
            Jvm.pause(100L);
            assertFalse(gotAppender.get());

            // Release
            acquireAppenderLock.lock();
            createAppenderCondition.signal();
            acquireAppenderLock.unlock();

            // Assert appender is acquired
            TimeoutPauser pauser = new TimeoutPauser(0);
            while (!gotAppender.get()) {
                pauser.pause(1, TimeUnit.SECONDS);
            }
        }
    }

    private static class MapWrapper extends SelfDescribingMarshallable {
        final Map<CharSequence, Double> map = new HashMap<>();
    }

    static class MyMarshable extends SelfDescribingMarshallable implements Demarshallable {
        @UsedViaReflection
        String name;

        @UsedViaReflection
        public MyMarshable(@NotNull WireIn wire) {
            readMarshallable(wire);
        }

        public MyMarshable() {
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
            bytes.releaseLast();
        }
    }
}
