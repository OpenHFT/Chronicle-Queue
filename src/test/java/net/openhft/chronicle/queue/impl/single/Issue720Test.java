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
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class Issue720Test extends ChronicleQueueTestBase {

    /*
                            Order of appearance
    Time   Message  Q       Expected Actual
    -2000  Text 0   queue0         0      0
    -2000  Text 1   queue1         1      1
    - 900  Text 2   queue2         2      4   This is moved in front of Text 3 and Text 4
    - 900  Text 3   queue3         3      2
    - 500  Text 4   queue3         4      3
    - 100  Text 5   queue3         5      5
      300  Text 6   queue3         6      6
      700  Text 7   queue3         7      7
    */

    @Test
    public void testTailerSnappingRollWithNewAppender2() {
        expectException("");
        final AtomicInteger cnt = new AtomicInteger();

        final SetTimeProvider timeProvider = new SetTimeProvider();
        timeProvider.currentTimeMillis(System.currentTimeMillis() - 2_000);
        final File dir = getTmpDir();
        final RollCycles rollCycle = RollCycles.TEST_SECONDLY;


        try (ChronicleQueue queue0 = binary(dir).rollCycle(rollCycle).timeProvider(timeProvider).build()) {
            final ExcerptAppender excerptAppender = queue0.acquireAppender();
            // write first message
            excerptAppender.writeText("Text " + cnt.getAndIncrement());
            // Affects the appender!
            printIndex(excerptAppender);

            try (ChronicleQueue queue1 = binary(dir).rollCycle(rollCycle).build()) {
                final ExcerptAppender appender1 = queue1.acquireAppender();
                appender1.writeText("Text " + cnt.getAndIncrement());

                printIndex(appender1);
            }

            // write second message
            try (ChronicleQueue queue2 = binary(dir).rollCycle(rollCycle).timeProvider(timeProvider).build()) {
                final ExcerptAppender appender2 = queue2.acquireAppender();
                for (int i = 0; i < 5; i++) {
                    System.out.println("i = " + i);
                    appender2.writeText("Text " + cnt.getAndIncrement());
                    printIndex(appender2);
                    timeProvider.advanceMillis(400);
                }
            }

        }

        System.out.println("READING");

        try (ChronicleQueue queue = binary(dir).rollCycle(rollCycle).timeProvider(timeProvider).build()) {
            final ExcerptTailer tailer = queue.createTailer();

            int readCnt = 0;
            for (String s = tailer.readText(); s != null; s = tailer.readText()) {
                // Fails!
                //assertEquals("Text "+readCnt++, s);
                System.out.println(s);
            }

        }

        int a = 0;
        //Jvm.pause(1000_000);

    }

    @Test
    public void ref() {
        expectException("");
        final AtomicInteger cnt = new AtomicInteger();

        final SetTimeProvider timeProvider = new SetTimeProvider();
        timeProvider.currentTimeMillis(System.currentTimeMillis() - 2_000);
        final File dir = getTmpDir();
        final RollCycles rollCycle = RollCycles.TEST_SECONDLY;

        try (ChronicleQueue queue0 = binary(dir).rollCycle(rollCycle).timeProvider(timeProvider).build()) {
            final ExcerptAppender excerptAppender = queue0.acquireAppender();
            excerptAppender.writeText("Text " + cnt.getAndIncrement());
            printIndex(excerptAppender);

            excerptAppender.writeText("Text " + cnt.getAndIncrement());
            printIndex(excerptAppender);

            for (int i = 0; i < 5; i++) {
                excerptAppender.writeText("Text " + cnt.getAndIncrement());
                printIndex(excerptAppender);
                timeProvider.advanceMillis(400);
            }
        }

        System.out.println("READING");

        try (ChronicleQueue queue = binary(dir).rollCycle(rollCycle).timeProvider(timeProvider).build()) {
            final ExcerptTailer tailer = queue.createTailer();

            int readCnt = 0;
            for (String s = tailer.readText(); s != null; s = tailer.readText()) {

                // This should not affect reading but does.
                // printIndex(tailer);

                assertEquals("Text " + readCnt++, s);
                System.out.println(s);
            }

        }
    }

    private void printIndex(ExcerptAppender appender) {
        // Affects the appender!
        /*
        try (final DocumentContext dc = appender.writingDocument()) {
            System.out.println("index = 0x" + Long.toHexString(dc.index()));
        }

         */
    }

    private void printIndex(ExcerptTailer tailer) {
        try (final DocumentContext dc = tailer.readingDocument()) {
            System.out.println("index = 0x" + Long.toHexString(dc.index()));
        }
    }

    @NotNull
    protected SingleChronicleQueueBuilder builder(@NotNull File file, @NotNull WireType wireType) {
        return SingleChronicleQueueBuilder.builder(file, wireType).rollCycle(RollCycles.TEST4_DAILY).testBlockSize();
    }

    @NotNull
    protected SingleChronicleQueueBuilder binary(@NotNull File file) {
        return builder(file, WireType.BINARY_LIGHT);
    }

}