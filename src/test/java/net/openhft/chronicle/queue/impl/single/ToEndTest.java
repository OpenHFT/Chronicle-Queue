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

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.onoes.ExceptionKey;
import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.*;

public class ToEndTest {
    private ThreadDump threadDump;
    private Map<ExceptionKey, Integer> exceptionKeyIntegerMap;

    @Before
    public void before() {
        threadDump = new ThreadDump();
        exceptionKeyIntegerMap = Jvm.recordExceptions();
    }

    @After
    public void after() {
        threadDump.assertNoNewThreads();

        Jvm.dumpException(exceptionKeyIntegerMap);
        assertTrue(exceptionKeyIntegerMap.isEmpty());
        Jvm.resetExceptionHandlers();
    }

    @Test
    public void missingCyclesToEndTest() throws InterruptedException {
        String path = OS.TARGET + "/missingCyclesToEndTest-" + System.nanoTime();
        IOTools.shallowDeleteDirWithFiles(path);

        final SetTimeProvider timeProvider = new SetTimeProvider();
        long now = 1470757797000L;
        long timeIncMs = 1001;
        timeProvider.currentTimeMillis(now);

        final RollingChronicleQueue queue = SingleChronicleQueueBuilder.binary(path)
                .rollCycle(RollCycles.TEST_SECONDLY)
                .timeProvider(timeProvider)
                .build();

        final ExcerptAppender appender = queue.acquireAppender();

        appender.writeDocument(wire -> wire.write(() -> "msg").int32(1));

        // roll
        timeProvider.currentTimeMillis(now += timeIncMs);

        appender.writeDocument(wire -> wire.write(() -> "msg").int32(2));
        appender.writeDocument(wire -> wire.write(() -> "msg").int32(3));


        final ExcerptTailer tailer = queue.createTailer().toEnd();
        try (DocumentContext dc = tailer.readingDocument()) {
            if (dc.isPresent()) {
                fail("Should be at the end of the queue but dc.isPresent and we read: " + String.valueOf(dc.wire().read(() -> "msg").int32()));
            }
        }

        // append same cycle.
        appender.writeDocument(wire -> wire.write(() -> "msg").int32(4));

        try (DocumentContext dc = tailer.readingDocument()) {
            assertTrue("Should be able to read entry in this cycle. Got NoDocumentContext.", dc.isPresent());
            int i = dc.wire().read(() -> "msg").int32();
            assertEquals("Should've read 4, instead we read: " + i, 4, i);
        }

        // read from the beginning
        tailer.toStart();

        for (int j = 1; j <= 4; j++) {
            try (DocumentContext dc = tailer.readingDocument()) {
                assertTrue(dc.isPresent());
                int i = dc.wire().read(() -> "msg").int32();
                assertEquals(j, i);
            }
        }

        try (DocumentContext dc = tailer.readingDocument()) {
            if (dc.isPresent()) {
                fail("Should be at the end of the queue but dc.isPresent and we read: " + String.valueOf(dc.wire().read(() -> "msg").int32()));
            }
        }

        // write another
        appender.writeDocument(wire -> wire.write(() -> "msg").int32(5));

        // roll 5 cycles
        timeProvider.currentTimeMillis(now += timeIncMs * 5);

        try (DocumentContext dc = tailer.readingDocument()) {
            assertTrue(dc.isPresent());
            assertEquals(5, dc.wire().read(() -> "msg").int32());
        }
        try (DocumentContext dc = tailer.readingDocument()) {
            assertFalse(dc.isPresent());
        }
    }

    @Test
    public void tailerToEndIncreasesRefCount() throws Exception {
        String path = OS.TARGET + "/toEndIncRefCount-" + System.nanoTime();
        IOTools.shallowDeleteDirWithFiles(path);

        SetTimeProvider time = new SetTimeProvider();
        long now = System.currentTimeMillis();
        time.currentTimeMillis(now);

        RollingChronicleQueue queue = SingleChronicleQueueBuilder.binary(path)
                .rollCycle(RollCycles.TEST_SECONDLY)
                .timeProvider(time)
                .build();

        final SingleChronicleQueueExcerpts.StoreAppender appender = (SingleChronicleQueueExcerpts.StoreAppender) queue.acquireAppender();
        Field storeF1 = SingleChronicleQueueExcerpts.StoreAppender.class.getDeclaredField("store");
        storeF1.setAccessible(true);
        SingleChronicleQueueStore store1 = (SingleChronicleQueueStore) storeF1.get(appender);
        System.out.println(store1);

        appender.writeDocument(wire -> wire.write(() -> "msg").int32(1));

        final SingleChronicleQueueExcerpts.StoreTailer tailer = (SingleChronicleQueueExcerpts.StoreTailer) queue.createTailer();
        System.out.println(tailer);
        tailer.toEnd();
        System.out.println(tailer);

        Field storeF2 = SingleChronicleQueueExcerpts.StoreTailer.class.getDeclaredField("store");
        storeF2.setAccessible(true);
        SingleChronicleQueueStore store2 = (SingleChronicleQueueStore) storeF2.get(tailer);

        // the reference count here is 2, one of the reference is the appender and on the tailer
        assertEquals(2, store2.refCount());
    }


    @Test
    public void toEndTest() {
        String baseDir = OS.TARGET + "/toEndTest-" + System.nanoTime();
        System.out.println(baseDir);
        IOTools.shallowDeleteDirWithFiles(baseDir);
        List<Integer> results = new ArrayList<>();
        RollingChronicleQueue queue = SingleChronicleQueueBuilder.binary(baseDir)
                .indexCount(8)
                .indexSpacing(1)
                .build();

        checkOneFile(baseDir);
        ExcerptAppender appender = queue.acquireAppender();
        checkOneFile(baseDir);

        for (int i = 0; i < 10; i++) {
            final int j = i;
            appender.writeDocument(wire -> wire.write(() -> "msg").int32(j));
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

        try {
            IOTools.shallowDeleteDirWithFiles(baseDir);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void toEndBeforeWriteTest() {
        String baseDir = OS.TARGET + "/toEndBeforeWriteTest";
        IOTools.shallowDeleteDirWithFiles(baseDir);

        ChronicleQueue queue = SingleChronicleQueueBuilder.binary(baseDir).build();
        checkOneFile(baseDir);

        // if this appender isn't created, the tailer toEnd doesn't cause a roll.
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

        /*for (int i = 0; i < 10; i++) {
            final int j = i;
            appender.writeDocument(wire -> wire.write(() -> "msg").int32(j));
        }*/
        IOTools.shallowDeleteDirWithFiles(baseDir);
    }

    @Test
    public void toEndAfterWriteTest() {
        String baseDir = OS.TARGET + "/toEndAfterWriteTest";
        IOTools.shallowDeleteDirWithFiles(baseDir);

        final SetTimeProvider stp = new SetTimeProvider();
        stp.currentTimeMillis(1470757797000L);

        ChronicleQueue wqueue = SingleChronicleQueueBuilder
                .binary(baseDir)
                .rollCycle(RollCycles.TEST_SECONDLY)
                .timeProvider(stp)
                .build();
        ExcerptAppender appender = wqueue.acquireAppender();
        for (int i = 0; i < 10; i++) {
            appender.writeText("hi-"+i);
            stp.currentTimeMillis(stp.currentTimeMillis() + 1000);
        }

        ChronicleQueue rqueue = SingleChronicleQueueBuilder
                .binary(baseDir)
                .rollCycle(RollCycles.TEST_SECONDLY)
                .timeProvider(stp)
                .build();

        ExcerptTailer tailer = rqueue.createTailer();
        stp.currentTimeMillis(stp.currentTimeMillis() + 1000);

        //noinspection StatementWithEmptyBody
        while (tailer.readText() != null) ;

        assertNull(tailer.readText());
        stp.currentTimeMillis(stp.currentTimeMillis() + 1000);

        assertNull(rqueue.createTailer().toEnd().readText());

        IOTools.shallowDeleteDirWithFiles(baseDir);
    }

    private void checkOneFile(String baseDir) {
        String[] files = new File(baseDir).list();

        if (files == null || files.length == 0)
            return;

        if (files.length == 1)
            assertTrue(files[0], files[0].startsWith("2"));
        else
            fail("Too many files " + Arrays.toString(files));
    }

    @NotNull
    private List<Integer> fillResults(ExcerptTailer tailer, List<Integer> results) {
        for (int i = 0; i < 10; i++) {

            try (DocumentContext documentContext = tailer.readingDocument()) {
                if (!documentContext.isPresent())
                    break;
                results.add(documentContext.wire().read(() -> "msg").int32());
            }

        }
        return results;
    }
}
