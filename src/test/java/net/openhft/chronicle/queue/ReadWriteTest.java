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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;

@RequiredForClient
public class ReadWriteTest extends QueueTestCommon {

    private static final String STR1 = "hello", STR2 = "hey";
    private File chroniclePath;

    @Before
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
    @After
    public void forceCleanupToDeFlakeTests() {
        BackgroundResourceReleaser.releasePendingResources();
    }

    @Override
    @Before
    public void threadDump() {
        super.threadDump();
    }

    @Test
    public void testReadFromReadOnlyChronicle() {
        assumeFalse(OS.isWindows());

        try (ChronicleQueue out = SingleChronicleQueueBuilder
                .binary(chroniclePath)
                .testBlockSize()
                .readOnly(true)
                .build()) {
            // check dump
            assertTrue(out.dump().length() > 1);
            // and tailer
            ExcerptTailer tailer = out.createTailer();
            assertEquals(STR1, tailer.readText());
            try (DocumentContext dc = tailer.readingDocument()) {
                assertEquals(STR2, dc.wire().bytes().readUtf8());
                // even though this is read-only we can still call dc.wire().bytes().write... which causes java.lang.InternalError
                // Fixing this in a type-safe manner would require on Read/WriteDocumentContext to return WireIn/WireOut
            }
        }
    }

    @Test
    public void testNotInitializedMetadataFile() throws IOException {
        assumeFalse(OS.isWindows());

        final String expectedException = "Failback to readonly tablestore";
        expectException(expectedException);
        System.out.println("This test will produce a " + expectedException);

        File meta = new File(chroniclePath, "metadata.cq4t");
        assertTrue(meta.exists());

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
            assertNotEquals(0, index);
        }
    }

    @Test
    public void testProceedWhenMetadataFileInitialized() throws IOException {
        assumeFalse(OS.isWindows());

        File meta = new File(chroniclePath, "metadata.cq4t");
        assertTrue(meta.exists());

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
                assumeNotNull(out);
                // Do nothing, just create
            }
        }).start();

        // the below can happen if the race mitigation code in TableDirectoryListingReadOnly.init is exercised
        // as a LongValue gets created before it can be assigned to a reference and be available to be closed
        ignoreException("Discarded without closing");
        try (ChronicleQueue out = SingleChronicleQueueBuilder
                .binary(chroniclePath)
                .testBlockSize()
                .readOnly(true)
                .build()) {

            assertTrue("Should have waited for more than 200ms. Actual wait: " + (System.currentTimeMillis() - startTimeMillis.get()) + " ms",
                    System.currentTimeMillis() - startTimeMillis.get() >= 200);

            ExcerptTailer tailer = out.createTailer();
            tailer.toEnd();
            long index = tailer.index();
            assertNotEquals(0, index);
        }
    }

    // Can't append to a read-only chronicle
    @Test(expected = IllegalStateException.class)
    public void testWriteToReadOnlyChronicle() {
        if (OS.isWindows()) {
            System.err.println("#460 Cannot test read only mode on windows");
            throw new IllegalStateException("not run");
        }

        try (ChronicleQueue out = SingleChronicleQueueBuilder
                .binary(chroniclePath)
                .testBlockSize()
                .readOnly(true)
                .build();
             final ExcerptAppender appender = out.createAppender()) {
            assumeNotNull(appender);
            // Do nothing
        }
    }

    @Test
    public void testToEndOnReadOnly() {
        assumeFalse("Read-only mode is not supported on Windows", OS.isWindows());

        try (ChronicleQueue out = SingleChronicleQueueBuilder
                .binary(chroniclePath)
                .testBlockSize()
                .readOnly(true)
                .build()) {
            ExcerptTailer tailer = out.createTailer();
            tailer.toEnd();
            long index = tailer.index();
            assertNotEquals(0, index);
        }
    }

    @Test
    public void testNonWriteableFilesSetToReadOnly() {
        assumeFalse(OS.isWindows());
        expectException("Failback to readonly tablestore");
        expectException("Forcing queue to be readOnly");

        Arrays.stream(chroniclePath.list()).forEach(s ->
                assertTrue(new File(chroniclePath, s).setWritable(false)));

        try (ChronicleQueue out = SingleChronicleQueueBuilder
                .binary(chroniclePath)
                .testBlockSize()
                .readOnly(false)
                .build()) {
            ExcerptTailer tailer = out.createTailer();
            tailer.toEnd();
            long index = tailer.index();
            assertNotEquals(0, index);
        }
    }
}
