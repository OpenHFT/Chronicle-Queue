/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Unit tests for ExcerptCommon interface implementations.
 */
public class ExcerptCommonTest extends QueueTestCommon {

    //! Each test owns a tracked directory; the old static path left metadata behind after the suite.
    //! Use the ordinary test block budget. Controls: testSourceId, testQueue, testCurrentFile, testSync.
    private ChronicleQueue newQueue() {
        return ChronicleQueue.singleBuilder(getTmpDir()).testBlockSize().build();
    }

    @Test
    public void ownsAndDeletesFixtureAfterSuccess() throws Exception {
        checkFixtureOwnership("normal");
    }

    @Test
    public void ownsAndDeletesFixtureAfterAssertionFailure() throws Exception {
        checkFixtureOwnership("assertion");
    }

    @Test
    public void ownsAndDeletesFixtureAfterConstructionFailure() throws Exception {
        checkFixtureOwnership("construction");
    }

    private void checkFixtureOwnership(String failureMode) throws Exception {
        final File[] owned = new File[1];
        final AssertionError bodyFailure = new AssertionError("injected fixture assertion");
        File unrelated = getTmpDir();
        java.nio.file.Files.createDirectories(unrelated.toPath());
        java.nio.file.Files.write(new File(unrelated, "keep").toPath(), new byte[]{1});
        ExcerptCommonTest fixture = new ExcerptCommonTest() {
            @Override protected File getTmpDir() {
                File path = super.getTmpDir();
                owned[0] = path;
                if ("construction".equals(failureMode)) {
                    try {
                        java.nio.file.Files.createDirectories(new File(path, "metadata.cq4t").toPath());
                    } catch (java.io.IOException e) {
                        throw new java.io.UncheckedIOException(e);
                    }
                }
                return path;
            }
        };
        ChronicleQueue opened = null;
        try {
            Throwable caught = null;
            try (ChronicleQueue queue = opened = fixture.newQueue()) {
                org.junit.Assert.assertEquals("factory path must be owned", owned[0], queue.file());
                org.junit.Assert.assertEquals("excerpt fixture mapping budget",
                        Math.max(net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.SMALL_BLOCK_SIZE,
                                32L * ((net.openhft.chronicle.queue.impl.single.SingleChronicleQueue) queue).indexCount()), ((net.openhft.chronicle.queue.impl.single.SingleChronicleQueue) queue).blockSize());
                if ("assertion".equals(failureMode))
                    throw bodyFailure;
            } catch (RuntimeException | AssertionError failure) {
                caught = failure;
            } finally {
                fixture.tearDown();
            }
            if ("normal".equals(failureMode))
                org.junit.Assert.assertNull(caught);
            else if ("assertion".equals(failureMode))
                org.junit.Assert.assertSame(bodyFailure, caught);
            else {
                org.junit.Assert.assertNotNull("construction must reject a directory as its metadata file", caught);
                org.junit.Assert.assertNull("construction failed before returning a queue", opened);
            }
            org.junit.Assert.assertNotNull("factory registered the original directory", owned[0]);
            org.junit.Assert.assertFalse("owned excerpt path remains", owned[0].exists());
            org.junit.Assert.assertTrue("unrelated path survives", new File(unrelated, "keep").isFile());
        } finally {
            if (opened != null) {
                opened.close();
                net.openhft.chronicle.core.io.BackgroundResourceReleaser.releasePendingResources();
                net.openhft.chronicle.core.io.IOTools.deleteDirWithFiles(opened.file());
            }
            if (owned[0] != null)
                net.openhft.chronicle.core.io.IOTools.deleteDirWithFiles(owned[0]);
        }
    }

    class ExcerptCommonImpl implements ExcerptCommon<ExcerptCommonImpl> {
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
            // no-op in stub: nothing to reset in this test
        }

        @Override
        public void singleThreadedCheckDisabled(boolean singleThreadedCheckDisabled) {
            // no-op in stub: single threaded check not relevant in this test
        }
    }

    @Test
    public void testSourceId() {
        try (ChronicleQueue queue = newQueue()) {
            ExcerptCommonImpl excerpt = new ExcerptCommonImpl(123, queue, null);
            assertEquals(123, excerpt.sourceId());
        }
    }

    @Test
    public void testQueue() {
        try (ChronicleQueue queue = newQueue()) {
            ExcerptCommonImpl excerpt = new ExcerptCommonImpl(123, queue, null);
            assertEquals(queue, excerpt.queue());
        }
    }

    @Test
    public void testCurrentFile() {
        File file = new File("testfile.txt");
        try (ChronicleQueue queue = newQueue()) {
            ExcerptCommonImpl excerpt = new ExcerptCommonImpl(123, queue, file);
            assertEquals(file, excerpt.currentFile());

            ExcerptCommonImpl excerptWithNullFile = new ExcerptCommonImpl(123, queue, null);
            assertNull(excerptWithNullFile.currentFile());
        }
    }

    @Test
    public void testSync() {
        try (ChronicleQueue queue = newQueue()) {
            ExcerptCommonImpl excerpt = new ExcerptCommonImpl(123, queue, null);
            excerpt.sync(); // Would test actual sync if implemented
            // Verify no state change and queue remains the same
            assertEquals(queue, excerpt.queue());
            assertEquals(123, excerpt.sourceId());
        }
    }
}
