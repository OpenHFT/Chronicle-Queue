/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.reader;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.PageUtil;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.io.ReferenceOwner;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.MetaDataKeys;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueStore;
import net.openhft.chronicle.queue.reader.ChronicleReader;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueue.SUFFIX;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_DAILY;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import org.junit.jupiter.api.Timeout;

@SuppressWarnings({"deprecation", "removal"})
public class RollEOFTest extends QueueTestCommon {

    private static final ReferenceOwner test = ReferenceOwner.temporary("test");

    @Nullable
    private static SingleChronicleQueueStore loadStore(@NotNull Wire wire) {
        final StringBuilder eventName = new StringBuilder();
        wire.readEventName(eventName);
        if (eventName.toString().equals(MetaDataKeys.header.name())) {
            final SingleChronicleQueueStore store = wire.read().typedMarshallable();
            if (store == null) {
                throw new IllegalArgumentException("Unable to load wire store");
            }
            return store;
        }

        Jvm.warn().on(RollEOFTest.class, "Unable to load store file from input. Queue file may be corrupted.");
        return null;
    }

    @Test
    @DisplayName("Rolling writes EOF and creates new roll files")
    @Timeout(value = 5000L, unit = TimeUnit.MILLISECONDS)
    public void testRollWritesEOF() throws IOException {
        assumeFalse(OS.isWindows(), "Read-only mode is not supported on Windows for roll EOF test");

        final File path = getTmpDir();
        try {
            path.mkdirs();
            final SetTimeProvider timeProvider = new SetTimeProvider();
            timeProvider.currentTimeMillis(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1));
            createQueueAndWriteData(timeProvider, path);
            assertEquals(1, getNumberOfQueueFiles(path), "Queue directory should contain one queue file after first write");

            // adjust time
            timeProvider.currentTimeMillis(System.currentTimeMillis());
            createQueueAndWriteData(timeProvider, path);
            assertEquals(2, getNumberOfQueueFiles(path), "Queue directory should contain two queue files after roll in write test");

            List<String> l = new LinkedList<>();
            new ChronicleReader().withMessageSink(l::add).withBasePath(path.toPath()).execute();
            // 2 entries per message
            assertEquals(4, l.size(), "Reader output should include four lines from two entries per message");
        } finally {
            IOTools.deleteDirWithFiles(path, 20);
        }
    }

    @Test
    @DisplayName("Roll without EOF remains readable in read-only mode")
    @Timeout(value = 5000L, unit = TimeUnit.MILLISECONDS)
    public void testRollWithoutEOFDoesntBlowup() throws IOException {
        assumeFalse(OS.isWindows(), "Read-only mode is not supported on Windows for roll without EOF");

        int messages = runRollWithoutEOF(-TimeUnit.DAYS.toMillis(1), true);
        assertEquals(4, messages, "roll without eof: messages read (read-only)");
    }

    @Test
    @DisplayName("Roll without EOF reads all messages in write mode")
    @Timeout(value = 5000L, unit = TimeUnit.MILLISECONDS)
    public void testRollWithoutEOF() throws IOException {
        // expectException("Overriding roll length from existing metadata");
        // expectException("Overriding roll cycle from");

        int messages = runRollWithoutEOF(-TimeUnit.DAYS.toMillis(3), false);
        assertEquals(4, messages, "roll without eof: messages read (writable)");
    }

    private int runRollWithoutEOF(long initialOffsetMillis, boolean readOnly) throws IOException {
        final File path = getTmpDir();
        try {
            path.mkdirs();
            final SetTimeProvider timeProvider = new SetTimeProvider();
            timeProvider.currentTimeMillis(System.currentTimeMillis() + initialOffsetMillis);
            createQueueAndWriteData(timeProvider, path);
            assertEquals(1, getNumberOfQueueFiles(path), "Queue directory should contain one queue file before roll");

            // adjust time
            timeProvider.currentTimeMillis(System.currentTimeMillis());
            createQueueAndWriteData(timeProvider, path);
            assertEquals(2, getNumberOfQueueFiles(path), "Queue directory should contain two queue files after roll for EOF removal");

            Optional<Path> firstQueueFile = Files.list(path.toPath()).filter(p -> p.toString().endsWith(SUFFIX)).sorted().findFirst();

            assertTrue(firstQueueFile.isPresent(), "first queue file should be present for EOF removal");

            // remove EOF from first file
            removeEOF(firstQueueFile.get());

            List<String> l = new LinkedList<>();
            ChronicleReader reader = new ChronicleReader().withMessageSink(l::add).withBasePath(path.toPath());
            if (!readOnly) {
                reader.withReadOnly(false);
            }
            reader.execute();
            // 2 entries per message
            return l.size();
        } finally {
            IOTools.deleteDirWithFiles(path, 20);
        }
    }

    private void removeEOF(Path path) throws IOException {
        long blockSize = OS.SAFE_PAGE_SIZE;
        long chunkSize = OS.pageAlign(blockSize);
        long overlapSize = OS.pageAlign(blockSize / 4);
        final MappedBytes mappedBytes = MappedBytes.mappedBytes(path.toFile(), chunkSize, overlapSize, PageUtil.getPageSize(path.toString()), false);
        mappedBytes.reserve(test);
        try {
            final Wire wire = WireType.BINARY_LIGHT.apply(mappedBytes);
            final Bytes<?> bytes = wire.bytes();
            bytes.readLimitToCapacity();
            bytes.readSkip(4);
            // move past header
            try (final SingleChronicleQueueStore qs = loadStore(wire)) {
                assertNotNull(qs, "queue store should load from wire");
                long l = qs.writePosition();
                long len = Wires.lengthOf(bytes.readVolatileInt(l));
                long eofOffset = l + len + 4L;
                bytes.writePosition(eofOffset);
                bytes.writeInt(0);
            }
        } finally {
            mappedBytes.release(test);
        }
    }

    private long getNumberOfQueueFiles(final File path) throws IOException {
        return getQueueFilesStream(path).count();
    }

    private Stream<Path> getQueueFilesStream(final File path) throws IOException {
        return Files.list(path.toPath()).filter(p -> p.toString().endsWith(SingleChronicleQueue.SUFFIX));
    }

    private void createQueueAndWriteData(TimeProvider timeProvider, File path) {

        try (final ChronicleQueue queue = SingleChronicleQueueBuilder
                .binary(path)
                .testBlockSize()
                .rollCycle(TEST_DAILY)
                .timeProvider(timeProvider)
                .build();
             ExcerptAppender excerptAppender = queue.createAppender()) {

            try (DocumentContext dc = excerptAppender.writingDocument(false)) {
                dc.wire().write("test").int64(0);
            }
        }
    }
}
