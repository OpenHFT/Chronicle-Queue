/*
 * Copyright 2016-2020 Chronicle Software
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
package net.openhft.chronicle.queue.impl.single.jira;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueTestBase;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * See https://higherfrequencytrading.atlassian.net/browse/QUEUE-36
 */
public class Queue36Test extends ChronicleQueueTestBase {
    @Test
    public void testTail() {
        File basePath = getTmpDir();
        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(basePath)
                .testBlockSize()
                .build()) {

            checkNoFiles(basePath);
            ExcerptTailer tailer = queue.createTailer();
            checkNoFiles(basePath);
            tailer.toStart();
            checkNoFiles(basePath);

            assertFalse(tailer.readDocument(d -> {
            }));

            checkNoFiles(basePath);
        }
    }

    private void checkNoFiles(@NotNull File basePath) {
        String[] fileNames = basePath.list((d, n) -> n.endsWith(SingleChronicleQueue.SUFFIX));
        assertTrue(fileNames == null || fileNames.length == 0);
    }
}
