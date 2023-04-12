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

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import static net.openhft.chronicle.queue.rollcycles.LegacyRollCycles.MINUTELY;
import static org.junit.Assume.assumeFalse;

public class StuckQueueTest extends QueueTestCommon {

    @Test
    public void test() throws IOException {


        // java.nio.file.InvalidPathException: Illegal char <:> at index 2: /D:/BuildAgent/work/1e5875c1db7235db/target/test-classes/stuck.queue.test/20180508-1249.cq4
        assumeFalse(OS.isWindows());

        Path tmpDir = getTmpDir().toPath();

        expectException("Failback to readonly tablestore");
        ignoreException("reading control code as text");
        expectException("Unexpected field lastAcknowledgedIndexReplicated");
//        expectException("Unable to copy TimedStoreRecovery safely will try anyway");
//        expectException("Unable to copy SCQStore safely will try anyway");
//        expectException("Unable to copy SCQSRoll safely");
//        expectException("Unable to copy SCQSIndexing safely");

        tmpDir.toFile().mkdirs();

        Path templatePath = Paths.get(StuckQueueTest.class.getResource("/stuck.queue.test/20180508-1249.cq4").getFile());
        Path to = tmpDir.resolve(templatePath.getFileName());
        Files.copy(templatePath, to, StandardCopyOption.REPLACE_EXISTING);

        try (RollingChronicleQueue q = ChronicleQueue.singleBuilder(tmpDir).rollCycle(MINUTELY).readOnly(true).build();
             ExcerptTailer tailer = q.createTailer()) {
//            System.out.println(q.dump());

            int cycle = q.rollCycle().toCycle(0x18406e100000000L);

            try (SingleChronicleQueueStore wireStore = q.storeForCycle(cycle, q.epoch(), false, null)) {
                String absolutePath = wireStore.file().getAbsolutePath();
                // System.out.println(absolutePath);
                Assert.assertTrue(absolutePath.endsWith("20180508-1249.cq4"));
            }

            // Assert.assertTrue(tailer.moveToIndex(0x18406e100000000L));

            try (DocumentContext dc = tailer.readingDocument()) {
                // Assert.assertTrue(!dc.isPresent());
                // System.out.println(Long.toHexString(dc.index()));
            }

            // Assert.assertTrue(tailer.moveToIndex(0x183efe300000000L));
            try (final SingleChronicleQueue q2 = ChronicleQueue.singleBuilder(tmpDir).rollCycle(MINUTELY).build()) {
                try (DocumentContext dc = q2.acquireAppender().writingDocument()) {
                    dc.wire().write("hello").text("world");
                }
            }
            ExcerptTailer tailer2 = q.createTailer();
            try (DocumentContext dc = tailer2.readingDocument()) {
                Assert.assertTrue(dc.isPresent());
                String actual = dc.wire().read("hello").text();
                Assert.assertEquals("world", actual);
                // System.out.println(Long.toHexString(dc.index()));
            }
        }
    }
}

