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

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Created by daniel on 07/03/2016.
 */
public class ToEndTest {
    private ThreadDump threadDump;

    @Before
    public void threadDump() {
        threadDump = new ThreadDump();
    }

    @After
    public void checkThreadDump() {
        threadDump.assertNoNewThreads();
    }

    @Test
    public void missingCyclesToEndTest() throws InterruptedException {
        String path = OS.TARGET + "/missingCyclesToEndTest-" + System.nanoTime();
        IOTools.shallowDeleteDirWithFiles(path);

        RollingChronicleQueue queue = SingleChronicleQueueBuilder.binary(path)
                .rollCycle(RollCycles.TEST_SECONDLY)
                .build();

        final ExcerptAppender appender = queue.acquireAppender();

        appender.writeDocument(wire -> wire.write(() -> "msg").int32(1));
        // Rolls one or two cycles ahead.
        Thread.sleep(1100);
        appender.writeDocument(wire -> wire.write(() -> "msg").int32(2));

        final ExcerptTailer tailer = queue.createTailer().toEnd();

        try (DocumentContext dc = tailer.readingDocument()) {
            assertFalse("We should be at the end of the queue. Instead we read: " + String.valueOf(dc.wire().read(() -> "msg").int32()), dc.isPresent());
        }

        // Roll another cycle or two.
        Thread.sleep(1100);
        appender.writeDocument(wire -> wire.write(() -> "msg").int32(3));

        try (DocumentContext dc = tailer.readingDocument()) {
            assertTrue("Should be able to read next entry! Instead NoDocumentContext.", dc.isPresent());
            int i = dc.wire().read(() -> "msg").int32();
            Assert.assertEquals("Should've read 3, instead we read: " + i, 3, i);
        }

        // now read from the beginning
        tailer.toStart();

        for (int j = 0; j <= 3; j++) {
            try (DocumentContext dc = tailer.readingDocument()) {
                assertTrue(dc.isPresent());
                int i = dc.wire().read(() -> "msg").int32();
                Assert.assertEquals(j++, i);
            }
        }
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
