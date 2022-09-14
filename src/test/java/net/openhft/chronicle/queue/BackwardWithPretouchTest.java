/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
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

package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BackwardWithPretouchTest extends ChronicleQueueTestBase {

    @Test
    public void testAppenderBackwardWithPretoucher() {
        test(1000);
    }

    @Test
    public void testAppenderBackwardWithPretoucherPause2Seconds() {
        test(2000);
    }

    @Test
    public void testAppenderBackwardWithPretoucherPause3Seconds() {
        test(3000);
    }

    private void test(final int pause) {
        expectException("This functionality has been deprecated and in future will only be available in Chronicle Queue Enterprise");
        SetTimeProvider timeProvider = new SetTimeProvider();
        timeProvider.currentTimeMillis(System.currentTimeMillis());
        File tmpDir = getTmpDir();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(tmpDir).timeProvider(timeProvider).rollCycle(RollCycles.TEST_SECONDLY).build()) {
            ExcerptAppender excerptAppender = queue.acquireAppender();
            try (DocumentContext dc = excerptAppender.writingDocument()) {
                dc.wire().write("hello").text("world");
            }

            timeProvider.advanceMillis(pause);

            int cycle;
            {
                ExcerptTailer tailer = queue.createTailer().direction(TailerDirection.BACKWARD);
               // System.out.println(Long.toHexString(tailer.index()));
                tailer.toEnd();
                cycle = tailer.cycle();
               // System.out.println(Long.toHexString(tailer.index()));
                try (DocumentContext dc = tailer.readingDocument()) {
                   // System.out.println(Long.toHexString(tailer.index()));
                    assertEquals("world", dc.wire().read("hello").text());
                }
            }

            // pretouch to create next cycle file  ----- IF YOU COMMENT THIS LINE THE TEST PASSES
            excerptAppender.pretouch();

            {
                ExcerptTailer tailer = queue.createTailer().direction(TailerDirection.BACKWARD);
               // System.out.println(Long.toHexString(tailer.index()));
                tailer.toEnd();
                assertEquals(cycle, tailer.cycle());
               // System.out.println(Long.toHexString(tailer.index()));
                try (DocumentContext dc = tailer.readingDocument()) {
                   // System.out.println(Long.toHexString(tailer.index()));
                    assertTrue(dc.isPresent());
                    assertEquals("world", dc.wire().read("hello").text());
                }
            }
        }
    }
}
