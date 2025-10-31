/*
 * Copyright 2016-2025 chronicle.software
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
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import static net.openhft.chronicle.queue.util.HugetlbfsTestUtil.getHugetlbfsQueueDirectory;
import static net.openhft.chronicle.queue.util.HugetlbfsTestUtil.isHugetlbfsAvailable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class HugetlbfsTest extends QueueTestCommon {

    @Rule
    public TestName testName = new TestName();

    @Test
    public void queueHugetlbfsEndToEndSimpleAcceptanceTest() {
        assumeTrue(isHugetlbfsAvailable());
        String path = getHugetlbfsQueueDirectory(testName);
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.single()
                .path(path)
                .build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer()) {
            appender.writeText("1");
            assertEquals("1", tailer.readText());
        } finally {
            IOTools.deleteDirWithFiles(path);
        }
    }
}
