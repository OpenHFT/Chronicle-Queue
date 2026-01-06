/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests for ExcerptCommon implementations covering identity, file handling, and no-op sync behaviour.
 */
@SuppressWarnings({"deprecation", "removal"})
public class ExcerptCommonTest extends QueueTestCommon {

    private static final String TEST_QUEUE = OS.getTarget() + "/ExcerptCommonTest";

    static class ExcerptCommonImpl implements ExcerptCommon<ExcerptCommonImpl> {
        private final int sourceId;
        private final ChronicleQueue queue;
        private final File currentFile;

        ExcerptCommonImpl(int sourceId, ChronicleQueue queue, File currentFile) {
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
        public void close() {
            // Close resources if necessary
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void singleThreadedCheckReset() {
            // no-op in stub: nothing to reset in this test
        }

        @Override
        public void singleThreadedCheckDisabled(boolean singleThreadedCheckDisabled) {
            // no-op in stub: single threaded check not relevant in this test
        }
    }

    @Test
    @DisplayName("Source ID reports constructor value for excerpt")
    public void testSourceId() {
        try (ChronicleQueue queue = ChronicleQueue.single(TEST_QUEUE)) {
            ExcerptCommonImpl excerpt = new ExcerptCommonImpl(123, queue, null);
            assertEquals(123, excerpt.sourceId(), "sourceId should return constructor value");
        }
    }

    @Test
    @DisplayName("Queue accessor returns provided instance reference")
    public void testQueue() {
        try (ChronicleQueue queue = ChronicleQueue.single(TEST_QUEUE)) {
            ExcerptCommonImpl excerpt = new ExcerptCommonImpl(123, queue, null);
            assertEquals(queue, excerpt.queue(), "queue should return provided instance");
        }
    }

    @Test
    @DisplayName("Current file accessor returns provided file or null")
    public void testCurrentFile() {
        File file = new File("testfile.txt");
        try (ChronicleQueue queue = ChronicleQueue.single(TEST_QUEUE)) {
            ExcerptCommonImpl excerpt = new ExcerptCommonImpl(123, queue, file);
            assertEquals(file, excerpt.currentFile(), "currentFile should return provided file reference");

            ExcerptCommonImpl excerptWithNullFile = new ExcerptCommonImpl(123, queue, null);
            assertNull(excerptWithNullFile.currentFile(), "currentFile should be null when unset");
        }
    }

    @Test
    @DisplayName("Sync leaves queue state unchanged for stub")
    public void testSync() {
        try (ChronicleQueue queue = ChronicleQueue.single(TEST_QUEUE)) {
            ExcerptCommonImpl excerpt = new ExcerptCommonImpl(123, queue, null);
            excerpt.sync(); // Would test actual sync if implemented
            // Verify no state change and queue remains the same
            assertEquals(queue, excerpt.queue(), "sync should not replace queue instance");
            assertEquals(123, excerpt.sourceId(), "sync should not alter sourceId");
        }
    }
}
