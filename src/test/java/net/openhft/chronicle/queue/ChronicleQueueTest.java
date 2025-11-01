/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.OutputStream;
import java.io.Writer;

import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings("deprecation")
public class ChronicleQueueTest extends QueueTestCommon {

    static final String PATH_NAME = OS.getTarget() + "/test-path";

    @AfterClass
    public static void cleanup() {
        // Clean up the test directory
        IOTools.deleteDirWithFiles(PATH_NAME);
    }

    @Test
    public void testSingleBuilderCreatesNewInstance() {
        // Test that the singleBuilder() method returns a valid SingleChronicleQueueBuilder instance
        SingleChronicleQueueBuilder builder = ChronicleQueue.singleBuilder();
        assertNotNull(builder);
    }

    @Test
    public void testIndexForIdThrowsUnsupportedOperationException() {
        // Test that indexForId(String id) throws an UnsupportedOperationException as expected
        try (ChronicleQueue queue = new StubChronicleQueue()) {
            assertThrows(UnsupportedOperationException.class, () -> queue.indexForId("someId"));
        }
    }

    @Test
    public void testCreateTailerThrowsUnsupportedOperationExceptionForNamedTailer() {
        // Test that createTailer(String id) throws an UnsupportedOperationException for the default implementation
        try (ChronicleQueue queue = new StubChronicleQueue()) {
            assertThrows(UnsupportedOperationException.class, () -> queue.createTailer("namedTailer"));
        }
    }

    @Test
    public void testCreateTailerCreatesNewExcerptTailer() {
        // Assuming createTailer() creates a valid ExcerptTailer when not overridden
        try (ChronicleQueue queue = ChronicleQueue.single(PATH_NAME);  // Adjust with a proper path
             ExcerptTailer tailer = queue.createTailer()) {
            assertNotNull(tailer);
        }
    }

    @Test
    public void testFileAbsolutePath() {
        // Assuming fileAbsolutePath() returns the correct absolute path of the Chronicle Queue
        try (ChronicleQueue queue = ChronicleQueue.single(PATH_NAME)) {  // Use a test path
            String path = queue.fileAbsolutePath();
            assertNotNull(path);
            assertTrue(path.replace('\\', '/').endsWith(PATH_NAME));  // Adjust based on actual path structure
        }
    }

    @Test
    public void testDumpCallsOutputStreamWriter() {
        // Test that dump(OutputStream stream, long fromIndex, long toIndex) calls the writer version correctly
        try (ChronicleQueue queue = new StubChronicleQueue() {

            @Override
            public void dump(Writer writer, long fromIndex, long toIndex) {
                // Validate that this method is called with the correct parameters
                assertNotNull(writer);
                assertEquals(0, fromIndex);
                assertEquals(10, toIndex);
            }
        }) {
            OutputStream stream = new ByteArrayOutputStream();
            queue.dump(stream, 0, 10);  // Expect that this calls the other dump method
        }
    }

    @Test
    public void testLastIndexReplicatedReturnsMinusOne() {
        // Test that lastIndexReplicated() returns -1
        try (ChronicleQueue queue = new StubChronicleQueue()) {
            assertEquals(-1, queue.lastIndexReplicated());
        }
    }

    @Test
    public void testLastAcknowledgedIndexReplicatedReturnsMinusOne() {
        // Test that lastAcknowledgedIndexReplicated() returns -1
        try (ChronicleQueue queue = new StubChronicleQueue()) {
            assertEquals(-1, queue.lastAcknowledgedIndexReplicated());
        }
    }

    @Test
    public void testLastIndexMSyncedReturnsMinusOne() {
        // Test that lastIndexMSynced() returns -1
        try (ChronicleQueue queue = new StubChronicleQueue()) {
            assertEquals(-1, queue.lastIndexMSynced());
        }
    }

    @Test
    public void testLastIndexMSyncedThrowsUnsupportedOperationException() {
        // Test that lastIndexMSynced(long lastIndexMSynced) throws UnsupportedOperationException
        try (ChronicleQueue queue = new StubChronicleQueue()) {
            assertThrows(UnsupportedOperationException.class, () -> queue.lastIndexMSynced(100L));
        }
    }

    @Test
    public void testAwaitAsyncReturnsTrue() {
        // Test that awaitAsync() always returns true
        try (ChronicleQueue queue = new StubChronicleQueue()) {
            assertTrue(queue.awaitAsync());
        }
    }

    @Test
    public void testNonAsyncTailerCallsCreateTailer() {
        // Test that nonAsyncTailer() calls createTailer()
        try (ChronicleQueue queue = ChronicleQueue.single(PATH_NAME)) {  // Adjust with a proper path
            assertNotNull(queue.nonAsyncTailer());
        }
    }

    // A minimal stub of ChronicleQueue for testing UnsupportedOperationException
    static class StubChronicleQueue implements ChronicleQueue {
        @Override
        public void close() {
            // Implementation for abstract method close()
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public @NotNull ExcerptTailer createTailer() {
            return null;
        }

        @Override
        public @NotNull ExcerptAppender createAppender() {
            return null;
        }

        @Override
        public long firstIndex() {
            return 0;
        }

        @Override
        public long lastIndex() {
            return 0;
        }

        @Override
        public @NotNull WireType wireType() {
            return null;
        }

        @Override
        public void clear() {
        }

        @Override
        public @NotNull File file() {
            return new File(PATH_NAME);
        }

        @Override
        public @NotNull String dump() {
            return null;
        }

        @Override
        public void dump(Writer writer, long fromIndex, long toIndex) {
        }

        @Override
        public int sourceId() {
            return 0;
        }

        @Override
        public @NotNull RollCycle rollCycle() {
            return null;
        }

        @Override
        public TimeProvider time() {
            return null;
        }

        @Override
        public int deltaCheckpointInterval() {
            return 0;
        }

        @Override
        public void lastIndexReplicated(long lastIndex) {
        }

        @Override
        public void lastAcknowledgedIndexReplicated(long lastAcknowledgedIndexReplicated) {
        }

        @Override
        public void refreshDirectoryListing() {
        }

        @Override
        public @NotNull String dumpLastHeader() {
            return null;
        }
    }
}
