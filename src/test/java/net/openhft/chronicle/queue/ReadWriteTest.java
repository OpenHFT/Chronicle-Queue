/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@RequiredForClient
public class ReadWriteTest extends QueueTestCommon {

    private static final String STR1 = "hello", STR2 = "hey";
    private File chroniclePath;

    @BeforeEach
    public void setup() {
        chroniclePath = getTmpDir();
        try (ChronicleQueue readWrite = ChronicleQueue.singleBuilder(chroniclePath)
                .readOnly(false)
                .testBlockSize()
                .build();
             final ExcerptAppender appender = readWrite.createAppender()) {
            appender.writeText(STR1);
            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().bytes().writeUtf8(STR2);
            }
        }
    }

    /**
     * Some flakiness with this test in build server due to background resources not released, we will revisit shortly
     * to deliver proper fix.
     */
    @AfterEach
    public void forceCleanupToDeFlakeTests() {
        BackgroundResourceReleaser.releasePendingResources();
    }

    @Override
    @BeforeEach
    public void threadDump() {
        super.threadDump();
    }

    @Test
    @DisplayName("Read-only queue can read existing data")
    public void testReadFromReadOnlyChronicle() {
        assumeFalse(OS.isWindows(), "Read-only queue test is not supported on Windows");

        try (ChronicleQueue out = SingleChronicleQueueBuilder
                .binary(chroniclePath)
                .testBlockSize()
                .readOnly(true)
                .build()) {
            // check dump
            assertTrue(out.dump().length() > 1, "Queue dump should produce non-empty output for read-only queue");
            // and tailer
            ExcerptTailer tailer = out.createTailer();
            assertEquals(STR1, tailer.readText(), "Tailer should read first text message from read-only queue");
            try (DocumentContext dc = tailer.readingDocument()) {
                assertEquals(STR2, dc.wire().bytes().readUtf8(), "Tailer should read second UTF-8 message from read-only queue via document context");
                // even though this is read-only we can still call dc.wire().bytes().write... which causes java.lang.InternalError
                // Fixing this in a type-safe manner would require on Read/WriteDocumentContext to return WireIn/WireOut
            }
        }
    }

    @Test
    @DisplayName("Read-only queue recovers after metadata truncation")
    public void testNotInitializedMetadataFile() throws IOException {
        assumeFalse(OS.isWindows(), "Metadata truncation test is not supported on Windows");

        final String expectedException = "Failback to readonly tablestore";
        expectException(expectedException);
        System.out.println("This test will produce a " + expectedException);

        File meta = new File(chroniclePath, "metadata.cq4t");
        assertTrue(meta.exists(), "Metadata file should exist after queue creation");

        try (RandomAccessFile raf = new RandomAccessFile(meta, "rw")) {
            raf.setLength(0);
        }

        try (ChronicleQueue out = SingleChronicleQueueBuilder
                .binary(chroniclePath)
                .testBlockSize()
                .readOnly(true)
                .build()) {

            ExcerptTailer tailer = out.createTailer();
            tailer.toEnd();
            long index = tailer.index();
            assertNotEquals(0, index, "Tailer index should advance for non-empty read-only queue");
        }
    }

    @Test
    @DisplayName("Read-only queue waits for metadata initialisation")
    public void testProceedWhenMetadataFileInitialized() throws IOException {
        assumeFalse(OS.isWindows(), "Metadata initialisation test is not supported on Windows");

        File meta = new File(chroniclePath, "metadata.cq4t");
        assertTrue(meta.exists(), "Metadata file should exist before truncation test");

        try (RandomAccessFile raf = new RandomAccessFile(meta, "rw")) {
            raf.setLength(0);
        }

        final AtomicLong startTimeMillis = new AtomicLong();
        new Thread(() -> {
            startTimeMillis.set(System.currentTimeMillis());
            Jvm.pause(200);
            try (ChronicleQueue out = SingleChronicleQueueBuilder
                    .binary(chroniclePath)
                    .testBlockSize()
                    .build()) {
                assertNotNull(out, "Queue should be successfully created after metadata initialisation completes");
            }
        }).start();

        // the below can happen if the race mitigation code in TableDirectoryListingReadOnly.init is exercised
        // as a LongValue gets created before it can be assigned to a reference and be available to be closed
        ignoreException("Discarded without closing");
        try (ChronicleQueue out = SingleChronicleQueueBuilder
                .binary(chroniclePath)
                .testBlockSize()
                .readOnly(true)
                .build();
             ExcerptTailer tailer = out.createTailer()) {

            long waitMillis = System.currentTimeMillis() - startTimeMillis.get();
            assertTrue(waitMillis >= 200, "Queue open should wait for at least 200 ms. Actual wait: " + waitMillis + " ms");

            tailer.toEnd();
            long index = tailer.index();
            assertNotEquals(0, index, "Tailer index should advance after metadata initialisation");
        }
    }

    // Can't append to a read-only chronicle
    @Test
    @DisplayName("Appender creation fails for read-only queue")
    public void testWriteToReadOnlyChronicle() {
        assumeFalse(OS.isWindows(), "#460 Read-only mode is not supported on Windows");
        assertThrows(IllegalStateException.class, () -> {
            try (ChronicleQueue out = SingleChronicleQueueBuilder
                    .binary(chroniclePath)
                    .testBlockSize()
                    .readOnly(true)
                    .build()) {
                ExcerptAppender appender = out.createAppender();
                appender.close();
            }
        }, "write: cannot append to a read-only queue");
    }

    @Test
    @DisplayName("Read-only tailer can move to end")
    public void testToEndOnReadOnly() {
        assumeFalse(OS.isWindows(), "Read-only mode is not supported on Windows");

        try (ChronicleQueue out = SingleChronicleQueueBuilder
                .binary(chroniclePath)
                .testBlockSize()
                .readOnly(true)
                .build()) {
            ExcerptTailer tailer = out.createTailer();
            tailer.toEnd();
            long index = tailer.index();
            assertNotEquals(0, index, "Tailer index should move past zero for read-only queue");
        }
    }

    @Test
    @DisplayName("Read-only mode is enforced when files are not writeable")
    public void testNonWriteableFilesSetToReadOnly() {
        assumeFalse(OS.isWindows(), "Read-only file permission test is not supported on Windows");
        expectException("Failback to readonly tablestore");
        expectException("Forcing queue to be readOnly");

        Arrays.stream(chroniclePath.list()).forEach(s ->
                assertTrue(new File(chroniclePath, s).setWritable(false), "Queue file should be successfully set to read-only mode"));

        try (ChronicleQueue out = SingleChronicleQueueBuilder
                .binary(chroniclePath)
                .testBlockSize()
                .readOnly(false)
                .build()) {
            ExcerptTailer tailer = out.createTailer();
            tailer.toEnd();
            long index = tailer.index();
            assertNotEquals(0, index, "Tailer index should advance when files are forced read-only");
        }
    }
}
