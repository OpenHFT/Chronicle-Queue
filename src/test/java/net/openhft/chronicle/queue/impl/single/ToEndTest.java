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

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.onoes.ExceptionKey;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.wire.DocumentContext;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.*;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.*;

public class ToEndTest extends ChronicleQueueTestBase {
    private static final long FIVE_SECONDS = SECONDS.toMicros(5);
    private static List<File> pathsToDelete = new LinkedList<>();
    long lastCycle;
    private Map<ExceptionKey, Integer> exceptionKeyIntegerMap;

    @AfterClass
    public static void afterClass() {
        for (File file : pathsToDelete) {
            IOTools.shallowDeleteDirWithFiles(file);
        }
    }

    @Test
    public void missingCyclesToEndTest() {
        String path = OS.getTarget() + "/missingCyclesToEndTest-" + Time.uniqueId();
        IOTools.shallowDeleteDirWithFiles(path);

        final SetTimeProvider timeProvider = new SetTimeProvider();
        long now = 1470757797000L;
        long timeIncMs = 1001;
        timeProvider.currentTimeMillis(now);

        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.binary(path)
                .testBlockSize()
                .rollCycle(RollCycles.TEST_SECONDLY)
                .timeProvider(timeProvider)
                .build()) {

            final ExcerptAppender appender = queue.acquireAppender();

            appender.writeDocument(wire -> wire.write("msg").int32(1));

            // roll
            timeProvider.currentTimeMillis(now += timeIncMs);

            appender.writeDocument(wire -> wire.write("msg").int32(2));
            appender.writeDocument(wire -> wire.write("msg").int32(3));

            final ExcerptTailer tailer = queue.createTailer().toEnd();
            try (DocumentContext dc = tailer.readingDocument()) {
                if (dc.isPresent()) {
                    fail("Should be at the end of the queue but dc.isPresent and we read: " + dc.wire().read("msg").int32());
                }
            }

            // append same cycle.
            appender.writeDocument(wire -> wire.write("msg").int32(4));

            try (DocumentContext dc = tailer.readingDocument()) {
                assertTrue("Should be able to read entry in this cycle. Got NoDocumentContext.", dc.isPresent());
                int i = dc.wire().read("msg").int32();
                assertEquals("Should've read 4, instead we read: " + i, 4, i);
            }

            // read from the beginning
            tailer.toStart();

            for (int j = 1; j <= 4; j++) {
                try (DocumentContext dc = tailer.readingDocument()) {
                    assertTrue(dc.isPresent());
                    int i = dc.wire().read("msg").int32();
                    assertEquals(j, i);
                }
            }

            try (DocumentContext dc = tailer.readingDocument()) {
                if (dc.isPresent()) {
                    fail("Should be at the end of the queue but dc.isPresent and we read: " + String.valueOf(dc.wire().read("msg").int32()));
                }
            }

            // write another
            appender.writeDocument(wire -> wire.write("msg").int32(5));

            // roll 5 cycles
            timeProvider.currentTimeMillis(now += timeIncMs * 5);

            try (DocumentContext dc = tailer.readingDocument()) {
                assertTrue(dc.isPresent());
                assertEquals(5, dc.wire().read("msg").int32());
            }
            try (DocumentContext dc = tailer.readingDocument()) {
                assertFalse(dc.isPresent());
            }
        }
    }

    @Test
    public void tailerToEndIncreasesRefCount() throws NoSuchFieldException, IllegalAccessException {
        String path = OS.getTarget() + "/toEndIncRefCount-" + Time.uniqueId();
        IOTools.shallowDeleteDirWithFiles(path);

        SetTimeProvider time = new SetTimeProvider();
        long now = System.currentTimeMillis();
        time.currentTimeMillis(now);

        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(path)
                .testBlockSize()
                .rollCycle(RollCycles.TEST_SECONDLY)
                .timeProvider(time)
                .build()) {

            final StoreAppender appender = (StoreAppender) queue.acquireAppender();
            Field storeF1 = StoreAppender.class.getDeclaredField("store");
            Jvm.setAccessible(storeF1);
            SingleChronicleQueueStore store1 = (SingleChronicleQueueStore) storeF1.get(appender);
            // System.out.println(store1);

            appender.writeDocument(wire -> wire.write("msg").int32(1));

            final StoreTailer tailer = (StoreTailer) queue.createTailer();
            // System.out.println(tailer);
            tailer.toEnd();
            // System.out.println(tailer);

            Field storeF2 = StoreTailer.class.getDeclaredField("store");
            Jvm.setAccessible(storeF2);
            SingleChronicleQueueStore store2 = (SingleChronicleQueueStore) storeF2.get(tailer);

            // the reference count here is 1, the queue itself
            assertFalse(store2.isClosed());
        }
    }

    @Test
    public void toEndTest() {
        File baseDir = getTmpDir();

        List<Integer> results = new ArrayList<>();
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(baseDir)
                .testBlockSize()
                .rollCycle(RollCycles.TEST_DAILY)
                .indexCount(8)
                .indexSpacing(1)
                .build()) {

            checkOneFile(baseDir);
            ExcerptAppender appender = queue.acquireAppender();
            checkOneFile(baseDir);

            for (int i = 0; i < 10; i++) {
                final int j = i;
                appender.writeDocument(wire -> wire.write("msg").int32(j));
            }

            checkOneFile(baseDir);

            ExcerptTailer tailer = queue.createTailer();
            checkOneFile(baseDir);

            ExcerptTailer atEnd = tailer.toEnd();
            assertEquals(10, queue.rollCycle().toSequenceNumber(atEnd.index()));
            checkOneFile(baseDir);
            fillResults(atEnd, results);
            checkOneFile(baseDir);
            assertEquals(0, results.size());

            tailer.toStart();
            checkOneFile(baseDir);
            fillResults(tailer, results);
            assertEquals(10, results.size());
            checkOneFile(baseDir);
        }
        System.gc();
        pathsToDelete.add(baseDir);
    }

    @Test
    public void toEndBeforeWriteTest() {
        File baseDir = getTmpDir();
        IOTools.shallowDeleteDirWithFiles(baseDir);

        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(baseDir)
                .testBlockSize()
                .build()) {
            checkOneFile(baseDir);

            // if this appender isn't created, the tailer toEnd doesn't cause a roll.
            @SuppressWarnings("unused")
            ExcerptAppender appender = queue.acquireAppender();
            checkOneFile(baseDir);

            ExcerptTailer tailer = queue.createTailer();
            checkOneFile(baseDir);

            ExcerptTailer tailer2 = queue.createTailer();
            checkOneFile(baseDir);

            tailer.toEnd();
            checkOneFile(baseDir);

            tailer2.toEnd();
            checkOneFile(baseDir);
        }
        System.gc();

        /*for (int i = 0; i < 10; i++) {
            final int j = i;
            appender.writeDocument(wire -> wire.write("msg").int32(j));
        }*/
        pathsToDelete.add(baseDir);
    }

    @Test
    public void toEndAfterWriteTest() {
        File file = getTmpDir();
        IOTools.shallowDeleteDirWithFiles(file);

        final SetTimeProvider stp = new SetTimeProvider();
        stp.currentTimeMillis(1470757797000L);

        try (ChronicleQueue wqueue = SingleChronicleQueueBuilder
                .binary(file)
                .testBlockSize()
                .rollCycle(RollCycles.TEST_SECONDLY)
                .timeProvider(stp)
                .build()) {
            ExcerptAppender appender = wqueue.acquireAppender();

            for (int i = 0; i < 10; i++) {
                try (DocumentContext dc = appender.writingDocument()) {
                    dc.wire().getValueOut().text("hi-" + i);
                    lastCycle = wqueue.rollCycle().toCycle(dc.index());
                }

                stp.currentTimeMillis(stp.currentTimeMillis() + 1000);
            }
        }

        try (ChronicleQueue rqueue = SingleChronicleQueueBuilder
                .binary(file)
                .testBlockSize()
                .rollCycle(RollCycles.TEST_SECONDLY)
                .timeProvider(stp)
                .build()) {

            ExcerptTailer tailer = rqueue.createTailer();
            stp.currentTimeMillis(stp.currentTimeMillis() + 1000);

            //noinspection StatementWithEmptyBody
            while (tailer.readText() != null) ;

            assertNull(tailer.readText());
            stp.currentTimeMillis(stp.currentTimeMillis() + 1000);

            ExcerptTailer tailer1 = rqueue.createTailer();
            ExcerptTailer excerptTailer = tailer1.toEnd();
            assertNull(excerptTailer.readText());
        }
        System.gc();
        pathsToDelete.add(file);
    }

    @Test
    public void shouldReturnZeroForEmptyQueue() {
        SetTimeProvider timeProvider = new SetTimeProvider();
        timeProvider.advanceMicros(FIVE_SECONDS);
        try (final SingleChronicleQueue queue = createQueue(timeProvider)) {
            assertEquals(0, getNextWriteIndex(queue));
        }
    }

    @Test
    public void shouldReturnZeroForEmptyPretouchedQueue() {
        SetTimeProvider timeProvider = new SetTimeProvider();
        timeProvider.advanceMicros(FIVE_SECONDS);
        try (final SingleChronicleQueue queue = createQueue(timeProvider)) {
            pretouchQueue(queue);
            assertEquals(0, getNextWriteIndex(queue));
        }
    }

    /**
     * This seems more like something that would only arise in testing,
     * but the behaviour is slightly different at index 0
     */
    @Test
    public void shouldReturnZeroForEmptyPretouchedQueueAtIndexZero() {
        SetTimeProvider timeProvider = new SetTimeProvider();
        try (final SingleChronicleQueue queue = createQueue(timeProvider)) {
            pretouchQueue(queue);
            assertEquals(0, getNextWriteIndex(queue));
        }
    }

    @Test
    public void shouldReturnZeroForQueueWithOnlyMetadata() {
        SetTimeProvider timeProvider = new SetTimeProvider();
        timeProvider.advanceMicros(FIVE_SECONDS);
        try (final SingleChronicleQueue queue = createQueue(timeProvider)) {
            writeMetadataToQueue(queue);
            assertEquals(0, getNextWriteIndex(queue));
        }
    }

    @Test
    public void shouldReturnNextWriteIndexForNonEmptyRolledByPretouch() {
        SetTimeProvider timeProvider = new SetTimeProvider();
        timeProvider.advanceMicros(FIVE_SECONDS);
        try (final SingleChronicleQueue queue = createQueue(timeProvider)) {
            writeExcerptToQueue(queue);
            long nextIndex = getNextWriteIndex(queue);

            timeProvider.advanceMicros(FIVE_SECONDS);

            pretouchQueue(queue);
            assertEquals(nextIndex, getNextWriteIndex(queue));
        }
    }

    @Test
    public void shouldReturnNextWriteIndexForNonEmptyRolledByMetadata() {
        SetTimeProvider timeProvider = new SetTimeProvider();
        timeProvider.advanceMicros(FIVE_SECONDS);
        try (final SingleChronicleQueue queue = createQueue(timeProvider)) {
            writeExcerptToQueue(queue);
            long nextIndex = getNextWriteIndex(queue);

            timeProvider.advanceMicros(FIVE_SECONDS);

            writeMetadataToQueue(queue);
            assertEquals(nextIndex, getNextWriteIndex(queue));
        }
    }

    private long getNextWriteIndex(SingleChronicleQueue queue) {
        try (final ExcerptTailer tailer = queue.createTailer().toEnd()) {
            return tailer.index();
        }
    }

    private void writeExcerptToQueue(SingleChronicleQueue queue) {
        try (final ExcerptAppender excerptAppender = queue.acquireAppender()) {
            excerptAppender.writeText("hello!");
        }
    }

    private void writeMetadataToQueue(SingleChronicleQueue queue) {
        try (final ExcerptAppender excerptAppender = queue.acquireAppender()) {
            try (final DocumentContext documentContext = excerptAppender.writingDocument(true)) {
                documentContext.wire().write().text("hello!");
            }
        }
    }

    private void pretouchQueue(SingleChronicleQueue queue) {
        try (final ExcerptAppender excerptAppender = queue.acquireAppender()) {
            excerptAppender.pretouch();
        }
    }

    private SingleChronicleQueue createQueue(SetTimeProvider timeProvider) {
        final File queueDir = getTmpDir();
        pathsToDelete.add(queueDir);
        return SingleChronicleQueueBuilder
                .binary(queueDir)
                .testBlockSize()
                .rollCycle(RollCycles.TEST_SECONDLY)
                .timeProvider(timeProvider)
                .build();
    }

    private void checkOneFile(@NotNull File baseDir) {
        String[] files = baseDir.list((d, n) -> n.endsWith(SingleChronicleQueue.SUFFIX));

        if (files == null || files.length == 0)
            return;

        if (files.length == 1)
            assertTrue(files[0], files[0].startsWith("2"));
        else
            fail("Too many files " + Arrays.toString(files));
    }

    @NotNull
    private List<Integer> fillResults(@NotNull ExcerptTailer tailer, @NotNull List<Integer> results) {
        for (int i = 0; i < 10; i++) {

            try (DocumentContext documentContext = tailer.readingDocument()) {
                if (!documentContext.isPresent())
                    break;
                results.add(documentContext.wire().read("msg").int32());
            }
        }
        return results;
    }

    @Before
    public void enableCloseableTracing() {
        AbstractCloseable.enableCloseableTracing();
    }

    @After
    public void assertCloseablesClosed() {
        AbstractCloseable.assertCloseablesClosed();
    }
}
