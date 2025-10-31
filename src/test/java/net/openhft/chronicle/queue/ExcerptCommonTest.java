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

import net.openhft.chronicle.core.OS;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Unit tests for ExcerptCommon interface implementations.
 */
public class ExcerptCommonTest extends QueueTestCommon {

    static final String TEST_QUEUE = OS.getTarget() + "/ExcerptCommonTest";

    class ExcerptCommonImpl implements ExcerptCommon<ExcerptCommonImpl> {
        private final int sourceId;
        private final ChronicleQueue queue;
        private final File currentFile;

        public ExcerptCommonImpl(int sourceId, ChronicleQueue queue, File currentFile) {
            this.sourceId = sourceId;
            this.queue = queue;
            this.currentFile = currentFile;
        }

        @Override
        public int sourceId() {
            return sourceId;
        }

        @Override
        public ChronicleQueue queue() {
            return queue;
        }

        @Override
        public File currentFile() {
            return currentFile;
        }

        @Override
        public void sync() {
            // Sync implementation
        }

        @Override
        public void close() {
            // Close resources if necessary
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void singleThreadedCheckReset() {

        }

        @Override
        public void singleThreadedCheckDisabled(boolean singleThreadedCheckDisabled) {

        }
    }

    @Test
    public void testSourceId() {
        try (ChronicleQueue queue = ChronicleQueue.single(TEST_QUEUE)) {
            ExcerptCommonImpl excerpt = new ExcerptCommonImpl(123, queue, null);
            assertEquals(123, excerpt.sourceId());
        }
    }

    @Test
    public void testQueue() {
        try (ChronicleQueue queue = ChronicleQueue.single(TEST_QUEUE)) {
            ExcerptCommonImpl excerpt = new ExcerptCommonImpl(123, queue, null);
            assertEquals(queue, excerpt.queue());
        }
    }

    @Test
    public void testCurrentFile() {
        File file = new File("testfile.txt");
        try (ChronicleQueue queue = ChronicleQueue.single(TEST_QUEUE)) {
            ExcerptCommonImpl excerpt = new ExcerptCommonImpl(123, queue, file);
            assertEquals(file, excerpt.currentFile());

            ExcerptCommonImpl excerptWithNullFile = new ExcerptCommonImpl(123, queue, null);
            assertNull(excerptWithNullFile.currentFile());
        }
    }

    @Test
    public void testSync() {
        try (ChronicleQueue queue = ChronicleQueue.single(TEST_QUEUE)) {
            ExcerptCommonImpl excerpt = new ExcerptCommonImpl(123, queue, null);
            excerpt.sync(); // Would test actual sync if implemented
            // No assertion needed for this default method
        }
    }
}
