/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.OutputStream;
import java.io.Writer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("deprecation")
class ChronicleQueueTest extends QueueTestCommon {

    private static final String PATH_NAME = OS.getTarget() + "/test-path";

    @AfterAll
    static void cleanup() {
        // Clean up the test directory
        IOTools.deleteDirWithFiles(PATH_NAME);
    }

    @Test
    @DisplayName("singleBuilder returns a new Chronicle queue builder instance")
    public void testSingleBuilderCreatesNewInstance() {
        // Test that the singleBuilder() method returns a valid SingleChronicleQueueBuilder instance
        SingleChronicleQueueBuilder builder = ChronicleQueue.singleBuilder();
        assertNotNull(builder, "Builder instance should be created by singleBuilder");
    }

    @Test
    @DisplayName("indexForId should throw unsupported operation exception")
    public void testIndexForIdThrowsUnsupportedOperationException() {
        // Test that indexForId(String id) throws an UnsupportedOperationException as expected
        try (ChronicleQueue queue = new StubChronicleQueue()) {
            assertThrows(UnsupportedOperationException.class, () -> queue.indexForId("someId"),
                    "indexForId should throw UnsupportedOperationException for stub queue");
        }
    }

    @Test
    @DisplayName("createTailer with id should throw unsupported")
    public void testCreateTailerThrowsUnsupportedOperationExceptionForNamedTailer() {
        // Test that createTailer(String id) throws an UnsupportedOperationException for the default implementation
        try (ChronicleQueue queue = new StubChronicleQueue()) {
            assertThrows(UnsupportedOperationException.class, () -> queue.createTailer("namedTailer"), "createTailer with id should be unsupported");
        }
    }

    @Test
    @DisplayName("createTailer should create a new queue tailer")
    public void testCreateTailerCreatesNewExcerptTailer() {
        // Assuming createTailer() creates a valid ExcerptTailer when not overridden
        try (ChronicleQueue queue = ChronicleQueue.single(PATH_NAME);  // Adjust with a proper path
             ExcerptTailer tailer = queue.createTailer()) {
            assertNotNull(tailer, "createTailer should return a fresh tailer instance");
        }
    }

    @Test
    @DisplayName("fileAbsolutePath should return absolute queue path")
    public void testFileAbsolutePath() {
        // Assuming fileAbsolutePath() returns the correct absolute path of the Chronicle Queue
        try (ChronicleQueue queue = ChronicleQueue.single(PATH_NAME)) {  // Use a test path
            String path = queue.fileAbsolutePath();
            assertNotNull(path, "fileAbsolutePath should return a path");
            assertTrue(path.replace('\\', '/').endsWith(PATH_NAME), "fileAbsolutePath should end with target path");
        }
    }

    @Test
    @DisplayName("dump should delegate to writer overload")
    public void testDumpCallsOutputStreamWriter() {
        // Test that dump(OutputStream stream, long fromIndex, long toIndex) calls the writer version correctly
        AtomicBoolean called = new AtomicBoolean();
        AtomicReference<Writer> writerRef = new AtomicReference<>();
        AtomicLong fromIndexRef = new AtomicLong();
        AtomicLong toIndexRef = new AtomicLong();
        try (ChronicleQueue queue = new StubChronicleQueue() {

            @Override
            public void dump(Writer writer, long fromIndex, long toIndex) {
                called.set(true);
                writerRef.set(writer);
                fromIndexRef.set(fromIndex);
                toIndexRef.set(toIndex);
            }
        }) {
            OutputStream stream = new ByteArrayOutputStream();
            queue.dump(stream, 0, 10);  // Expect that this calls the other dump method
            assertTrue(called.get(), "dump should call writer overload for OutputStream");
            assertNotNull(writerRef.get(), "dump should pass a writer instance");
            assertEquals(0L, fromIndexRef.get(), "dump should pass expected fromIndex");
            assertEquals(10L, toIndexRef.get(), "dump should pass expected toIndex");
        }
    }

    @Test
    @DisplayName("lastIndexReplicated should return minus one default sentinel")
    public void testLastIndexReplicatedReturnsMinusOne() {
        // Test that lastIndexReplicated() returns -1
        try (ChronicleQueue queue = new StubChronicleQueue()) {
            assertEquals(-1, queue.lastIndexReplicated(), "Stub queue lastIndexReplicated should return -1");
        }
    }

    @Test
    @DisplayName("lastAcknowledgedIndexReplicated should return minus one default sentinel")
    public void testLastAcknowledgedIndexReplicatedReturnsMinusOne() {
        // Test that lastAcknowledgedIndexReplicated() returns -1
        try (ChronicleQueue queue = new StubChronicleQueue()) {
            assertEquals(-1, queue.lastAcknowledgedIndexReplicated(), "Stub queue lastAcknowledgedIndexReplicated should return -1");
        }
    }

    @Test
    @DisplayName("lastIndexMSynced should return minus one default sentinel")
    public void testLastIndexMSyncedReturnsMinusOne() {
        // Test that lastIndexMSynced() returns -1
        try (ChronicleQueue queue = new StubChronicleQueue()) {
            assertEquals(-1, queue.lastIndexMSynced(), "Stub queue lastIndexMSynced should return -1");
        }
    }

    @Test
    @DisplayName("lastIndexMSynced setter should throw unsupported exception")
    public void testLastIndexMSyncedThrowsUnsupportedOperationException() {
        // Test that lastIndexMSynced(long lastIndexMSynced) throws UnsupportedOperationException
        try (ChronicleQueue queue = new StubChronicleQueue()) {
            assertThrows(UnsupportedOperationException.class, () -> queue.lastIndexMSynced(100L),
                    "lastIndexMSynced setter should reject updates on stub queue");
        }
    }

    @Test
    @DisplayName("awaitAsync should return true for stub queue calls")
    public void testAwaitAsyncReturnsTrue() {
        // Test that awaitAsync() always returns true
        try (ChronicleQueue queue = new StubChronicleQueue()) {
            assertTrue(queue.awaitAsync(), "Stub queue awaitAsync should return true");
        }
    }

    @Test
    @DisplayName("nonAsyncTailer should create a queue tailer")
    public void testNonAsyncTailerCallsCreateTailer() {
        // Test that nonAsyncTailer() calls createTailer()
        try (ChronicleQueue queue = ChronicleQueue.single(PATH_NAME)) {  // Adjust with a proper path
            assertNotNull(queue.nonAsyncTailer(), "nonAsyncTailer should return a tailer instance");
        }
    }

    // A minimal stub of ChronicleQueue for testing UnsupportedOperationException
    static class StubChronicleQueue implements ChronicleQueue {
        @Override
        public void close() {
            // no-op in stub: this queue is a lightweight test double
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
            // no-op in stub: clearing not required for test scenarios
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
            // no-op in stub: replication not exercised in this test
        }

        @Override
        public void lastAcknowledgedIndexReplicated(long lastAcknowledgedIndexReplicated) {
            // no-op in stub: replication not exercised in this test
        }

        @Override
        public void refreshDirectoryListing() {
            // no-op in stub: directory listing not needed for this test
        }

        @Override
        public @NotNull String dumpLastHeader() {
            return null;
        }
    }
}
