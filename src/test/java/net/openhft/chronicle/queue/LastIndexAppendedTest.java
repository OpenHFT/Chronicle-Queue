/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.wire.DocumentContext;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.File;

import static net.openhft.chronicle.queue.RollCycles.TEST_DAILY;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.single;
import static org.junit.Assert.*;

@RequiredForClient
public class LastIndexAppendedTest extends QueueTestCommon {

    @Test
    public void testLastIndexAppendedAcrossRestarts() {
        String path = OS.TARGET + "/" + getClass().getSimpleName() + "-" + System.nanoTime();

        for (int i = 0; i < 5; i++) {
            try (ChronicleQueue queue = single(path)
                    .testBlockSize()
                    .rollCycle(TEST_DAILY)
                    .build()) {
                ExcerptAppender appender = queue.acquireAppender();

                try (DocumentContext documentContext = appender.writingDocument()) {
                    int index = (int) documentContext.index();
                    assertEquals(i, index);

                    documentContext.wire().write().text("hello world");
                }

                assertEquals(i, (int) appender.lastIndexAppended());
            }
        }
        try {
            IOTools.deleteDirWithFiles(path, 2);
        } catch (Exception index) {
        }
    }

    @Test
    public void testTwoAppenders() {
        File path = DirectoryUtils.tempDir("testTwoAppenders");
        long a_index;

        try (
                ChronicleQueue appender_queue = single(path)
                        .testBlockSize()
                        .rollCycle(TEST_DAILY)
                        .build()) {
            ExcerptAppender appender = appender_queue.acquireAppender();
            for (int i = 0; i < 5; i++) {
                appender.writeDocument(wireOut -> wireOut.write("log").marshallable(m ->
                        m.write("msg").text("hello world ")));
            }
            a_index = appender.lastIndexAppended();
        }
        try (ChronicleQueue tailer_queue = single(path)
                .testBlockSize()
                .rollCycle(TEST_DAILY)
                .build()) {
            ExcerptTailer tailer = tailer_queue.createTailer();
            tailer = tailer.toStart();
            long t_index;
            t_index = doRead(tailer, 5);
            assertEquals(a_index, t_index);
//            System.out.println("Continue appending");
            try (ChronicleQueue appender_queue = single(path)
                            .testBlockSize()
                            .rollCycle(TEST_DAILY)
                            //.buffered(false)
                            .build()) {
                ExcerptAppender appender = appender_queue.acquireAppender();
                for (int i = 0; i < 5; i++) {
                    appender.writeDocument(wireOut -> wireOut.write("log").marshallable(m ->
                            m.write("msg").text("hello world2 ")));
                }
                a_index = appender.lastIndexAppended();
                assertTrue(a_index > t_index);
            }
            // if the tailer continues as well it should see the 5 new messages
//            System.out.println("Reading messages added");
            t_index = doRead(tailer, 5);
            assertEquals(a_index, t_index);

            // if the tailer is expecting to read all the message again
//            System.out.println("Reading all the messages again");
            tailer.toStart();
            t_index = doRead(tailer, 10);
            assertEquals(a_index, t_index);
        }
        try {
            IOTools.deleteDirWithFiles(path, 2);
        } catch (Exception index) {
        }
    }

    private long doRead(@NotNull ExcerptTailer tailer, int expected) {
        int[] i = {0};
        long t_index = 0;
        while (true) {
            try (DocumentContext dc = tailer.readingDocument()) {
                if (!dc.isPresent())
                    break;
                t_index = tailer.index();
                dc.wire().read("log").marshallable(m -> {
                    String msg = m.read("msg").text();
                    assertNotNull(msg);
//                    System.out.println("msg:" + msg);
                    i[0]++;
                });
            }
        }
        assertEquals(expected, i[0]);
        return t_index;
    }

}
