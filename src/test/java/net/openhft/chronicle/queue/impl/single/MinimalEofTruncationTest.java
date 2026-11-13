/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.bytes.internal.ChunkedMappedFile;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.onoes.ExceptionHandler;
import net.openhft.chronicle.core.onoes.ThreadLocalisedExceptionHandler;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.Wires;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeFalse;

public class MinimalEofTruncationTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    @Test public void saturatedReleaserSkipsInlineCleanupAndReleasesReservation() throws Exception {
        assumeFalse(OS.isWindows());
        BackgroundResourceReleaser.releasePendingResources();
        CountDownLatch started = new CountDownLatch(1), release = new CountDownLatch(1);
        File file = paddedFile();
        Bytes<?> bytes = Bytes.allocateElasticOnHeap(128);
        try (MappedFile mapped = MappedFile.mappedFile(file, 65536, 0)) {
            bytes.writeInt(Wires.END_OF_DATA).writeInt(0x464f45c3);
            int references = mapped.refCount();
            BackgroundResourceReleaser.run(() -> {
                started.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            try {
                assertTrue(started.await(5, TimeUnit.SECONDS));
                for (int i = 0; i < 128; i++)
                    BackgroundResourceReleaser.run(() -> { });
                SingleChronicleQueueStore.truncateIfEOF(new BinaryWire(bytes), mapped);
                assertEquals(65536, file.length());
                assertEquals(references, mapped.refCount());
            } finally {
                release.countDown();
                BackgroundResourceReleaser.releasePendingResources();
            }
            SingleChronicleQueueStore.truncateIfEOF(new BinaryWire(bytes), mapped);
            BackgroundResourceReleaser.releasePendingResources();
            assertEquals(OS.pageAlign(8) + OS.pageSize(), file.length());
            assertEquals(references, mapped.refCount());
        } finally {
            release.countDown();
            bytes.releaseLast();
        }
    }

    @Test public void completeMarkerWorksAtFourByteAndPageBoundaries() throws Exception {
        for (long offset : new long[]{120, 124, OS.pageSize() - 4}) {
            File file = paddedFile();
            try (MappedBytes bytes = MappedBytes.mappedBytes(file, 65536)) {
                bytes.writePosition(offset);
                BinaryWire wire = new BinaryWire(bytes);
                assertTrue(wire.writeEndOfWire(1, TimeUnit.MILLISECONDS, offset));
                assertEquals(offset + 8, bytes.writePosition());
                long padded = file.length();
                SingleChronicleQueueStore.truncateIfEOF(wire, bytes.mappedFile());
                BackgroundResourceReleaser.releasePendingResources();
                assertEquals(OS.isWindows() ? padded : OS.pageAlign(offset + 8) + OS.pageSize(), file.length());
            }
        }
    }
    @Test public void platformOutcomePreservesTheFileReservation() throws Exception {
        File file = paddedFile();
        Bytes<?> bytes = Bytes.allocateElasticOnHeap(128);
        try (MappedFile mapped = MappedFile.mappedFile(file, 65536, 0)) {
            bytes.writeInt(Wires.END_OF_DATA).writeInt(0x464f45c3);
            int references = mapped.refCount();
            SingleChronicleQueueStore.truncateIfEOF(new BinaryWire(bytes), mapped);
            BackgroundResourceReleaser.releasePendingResources();
            assertEquals(references, mapped.refCount());
            assertEquals(OS.isWindows() ? 65536 : OS.pageAlign(8) + OS.pageSize(), file.length());
        } finally { bytes.releaseLast(); }
    }
    @Test public void shortOrCorruptMarkerIsPreservedAndDiagnosedAtDebug() throws Exception {
        File file = paddedFile();
        Bytes<?> bytes = Bytes.allocateElasticOnHeap(128);
        List<String> messages = new ArrayList<>();
        ThreadLocalisedExceptionHandler debug = (ThreadLocalisedExceptionHandler) Jvm.debug();
        ExceptionHandler previous = debug.threadLocalHandler();
        debug.threadLocalHandler((type, message, failure) -> messages.add(message));
        try {
            for (int end = 0; end < 8; end++) {
                messages.clear();
                bytes.writePosition(end);
                request(bytes, file);
                BackgroundResourceReleaser.releasePendingResources();
                assertEquals(65536, file.length());
                assertTrue(messages.toString(), messages.stream().anyMatch(m -> m.contains("fewer than 8 bytes (" + bytes.writePosition() + ")")));
            }
            bytes.clear();
            BinaryWire wire = new BinaryWire(bytes);
            assertTrue(wire.writeEndOfWire(1, TimeUnit.MILLISECONDS, 0));
            long complete = bytes.readLong(0);
            // Flip every header/annotation bit, then try a header with no annotation.
            for (int bit = 0; bit <= 64; bit++) {
                long corrupt = bit == 64 ? Wires.END_OF_DATA & 0xffffffffL : complete ^ (1L << bit);
                bytes.writeLong(0, corrupt);
                messages.clear();
                request(bytes, file);
                BackgroundResourceReleaser.releasePendingResources();
                assertEquals(65536, file.length());
                String expected = String.format("last8=0x%016x", corrupt);
                assertTrue(messages.toString(), messages.stream().anyMatch(m -> m.contains(expected)));
            }
        } finally { debug.threadLocalHandler(previous); bytes.releaseLast(); }
    }
    @Test public void noEofDoesNotTruncate() throws Exception {
        File file = paddedFile();
        Bytes<?> bytes = Bytes.allocateElasticOnHeap(128);
        try {
            bytes.writeLong(42);
            request(bytes, file);
            BackgroundResourceReleaser.releasePendingResources();
            assertEquals(65536, file.length());
        } finally { bytes.releaseLast(); }
    }
    @Test public void eofTruncatesAndDoesNotGrowAnAlreadyShorterFile() throws Exception {
        assumeFalse("Live mapped-file truncation is deferred on Windows", OS.isWindows());
        File file = paddedFile();
        Bytes<?> bytes = Bytes.allocateElasticOnHeap(128);
        try {
            bytes.writePosition(120).writeInt(Wires.END_OF_DATA).writeInt(0x464f45c3);
            request(bytes, file);
            BackgroundResourceReleaser.releasePendingResources();
            assertEquals(OS.pageAlign(128) + OS.pageSize(), file.length());
            try (RandomAccessFile out = new RandomAccessFile(file, "rw")) { out.setLength(64); }
            request(bytes, file);
            BackgroundResourceReleaser.releasePendingResources();
            assertEquals(64, file.length());
        } finally { bytes.releaseLast(); }
    }
    @Test public void readOnlyChannelIsNotTruncatedOrThrownToCaller() throws Exception {
        File file = paddedFile();
        Bytes<?> bytes = Bytes.allocateElasticOnHeap(128);
        try (MappedFile mapped = MappedFile.of(file, 65536, 0, true)) {
            bytes.writeInt(Wires.END_OF_DATA).writeInt(0x464f45c3);
            SingleChronicleQueueStore.truncateIfEOF(new BinaryWire(bytes), mapped);
            BackgroundResourceReleaser.releasePendingResources();
            assertEquals(65536, file.length());
        } finally { bytes.releaseLast(); }
    }
    @Test public void backgroundFailureWarnsAndReleasesReservationEvenForErrors() throws Exception {
        assumeFalse("No truncation task is scheduled on Windows", OS.isWindows());
        for (Throwable failure : new Throwable[]{new IllegalStateException("injected"), new AssertionError("injected")}) {
            File file = paddedFile();
            Bytes<?> bytes = Bytes.allocateElasticOnHeap(128);
            List<Throwable> warnings = Collections.synchronizedList(new ArrayList<>());
            ThreadLocalisedExceptionHandler warn = (ThreadLocalisedExceptionHandler) Jvm.warn();
            ExceptionHandler previous = warn.defaultHandler();
            Jvm.setWarnExceptionHandler((type, message, error) -> {
                if (type.getName().equals(SingleChronicleQueueStore.class.getName()) && message.equals("Unable to truncate " + file))
                    warnings.add(error);
                else
                    previous.on(type, message, error);
            });
            try (RandomAccessFile raf = new RandomAccessFile(file, "rw");
                 MappedFile mapped = new ChunkedMappedFile(file, raf, 65536, 0, 65536, false) {
                     @Override public RandomAccessFile raf() {
                         throw Jvm.rethrow(failure);
                     }
                 }) {
                bytes.writeInt(Wires.END_OF_DATA).writeInt(0x464f45c3);
                int references = mapped.refCount();
                SingleChronicleQueueStore.truncateIfEOF(new BinaryWire(bytes), mapped);
                BackgroundResourceReleaser.releasePendingResources();
                assertEquals(Collections.singletonList(failure), warnings);
                assertEquals(references, mapped.refCount());
                assertEquals(65536, file.length());
                request(bytes, file);
                BackgroundResourceReleaser.releasePendingResources();
                assertEquals(OS.pageAlign(8) + OS.pageSize(), file.length());
            } finally { Jvm.setWarnExceptionHandler(previous); bytes.releaseLast(); }
        }
    }
    @Test public void twoWritersRollResumeAndReadBack() throws Exception {
        File dir = temporary.newFolder();
        SetTimeProvider time = new SetTimeProvider(1000);
        try (SingleChronicleQueue first = queue(dir, time);
             SingleChronicleQueue second = queue(dir, time);
             ExcerptAppender a = first.createAppender();
             ExcerptAppender b = second.createAppender()) {
            a.writeText("a-before"); b.writeText("b-before");
            File old = Objects.requireNonNull(dir.listFiles((d,n) -> n.endsWith(".cq4")))[0];
            long padded = old.length();
            time.advanceMillis(1000); a.writeText("a-after");
            BackgroundResourceReleaser.releasePendingResources();
            if (OS.isWindows())
                assertEquals("Windows must defer live truncation", padded, old.length());
            else
                assertTrue("completed cycle should shrink", old.length() < padded);
            b.writeText("b-after");
            try (ExcerptTailer tailer = first.createTailer()) {
                assertEquals("a-before", tailer.readText()); assertEquals("b-before", tailer.readText());
                assertEquals("a-after", tailer.readText()); assertEquals("b-after", tailer.readText());
                assertNull(tailer.readText());
            }
        } finally { BackgroundResourceReleaser.releasePendingResources(); }
    }
    private void request(Bytes<?> bytes, File file) throws IOException {
        try (MappedFile mapped = MappedFile.mappedFile(file, 65536, 0)) {
            SingleChronicleQueueStore.truncateIfEOF(new BinaryWire(bytes), mapped);
        }
    }
    private File paddedFile() throws IOException {
        File file = temporary.newFile();
        try (RandomAccessFile out = new RandomAccessFile(file,"rw")) { out.setLength(65536); }
        return file;
    }
    static SingleChronicleQueue queue(File dir, SetTimeProvider time) {
        return SingleChronicleQueueBuilder.binary(dir).blockSize(262144)
                .rollCycle(TestRollCycles.TEST_SECONDLY).timeProvider(time).build();
    }
}
