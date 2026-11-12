/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.main;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.Wires;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import static net.openhft.chronicle.queue.internal.main.OfflineCycleFileTruncator.Outcome.*;
import static org.junit.Assert.*;

public class OfflineCycleFileTruncatorTest {
    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void noEofLeavesEveryByteUnchanged() throws IOException {
        Path file = fixture(1, false);
        byte[] before = Files.readAllBytes(file);
        assertEquals(NO_EOF, OfflineCycleFileTruncator.truncate(file, true));
        assertArrayEquals(before, Files.readAllBytes(file));
    }

    @Test
    public void payloadEofIsNotARecordBoundary() throws IOException {
        Path file = fixture(1, false);
        byte[] data = Files.readAllBytes(file);
        int offset = tailOffset(data) - 8;
        ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).putInt(offset, 8);
        System.arraycopy(new byte[]{(byte) 0xc3, 'E', 'O', 'F'}, 0, data, offset + 8, 4);
        Files.write(file, data);
        assertEquals(NO_EOF, OfflineCycleFileTruncator.truncate(file, true));
        assertArrayEquals(data, Files.readAllBytes(file));
    }

    @Test
    public void eofRequiresOfflineConfirmation() throws IOException {
        Path file = fixture(1, true);
        byte[] before = Files.readAllBytes(file);
        assertEquals(OFFLINE_CONFIRMATION_REQUIRED, OfflineCycleFileTruncator.truncate(file, false));
        assertArrayEquals(before, Files.readAllBytes(file));
    }

    @Test
    public void trimsOnlyAfterEofAndIsIdempotent() throws IOException {
        Path file = fixture(1, true);
        byte[] before = Files.readAllBytes(file);
        long end;
        try (FileChannel channel = FileChannel.open(file)) {
            end = CycleFileEnd.find(channel);
        }
        assertEquals(TRUNCATED, OfflineCycleFileTruncator.truncate(file, true));
        assertArrayEquals(Arrays.copyOf(before, (int) end), Files.readAllBytes(file));
        assertEquals(ALREADY_TRIMMED, OfflineCycleFileTruncator.truncate(file, true));
    }

    @Test
    public void finalisationMakesDeferredFileEligible() throws IOException {
        Path file = fixture(1, false);
        assertEquals(NO_EOF, OfflineCycleFileTruncator.truncate(file, true));
        byte[] bytes = Files.readAllBytes(file);
        int offset = tailOffset(bytes);
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).putInt(offset, Wires.END_OF_DATA);
        System.arraycopy(new byte[]{(byte) 0xc3, 'E', 'O', 'F'}, 0, bytes, offset + 4, 4);
        Files.write(file, bytes);
        assertEquals(TRUNCATED, OfflineCycleFileTruncator.truncate(file, true));
    }

    @Test
    public void incompleteRecordIsNotEof() throws IOException {
        Path file = fixture(1, false);
        byte[] bytes = Files.readAllBytes(file);
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).putInt(tailOffset(bytes), Wires.NOT_COMPLETE);
        Files.write(file, bytes);
        assertEquals(NO_EOF, OfflineCycleFileTruncator.truncate(file, true));
        assertArrayEquals(bytes, Files.readAllBytes(file));
    }

    @Test
    public void partialEofIsNotEligible() throws IOException {
        Path file = fixture(1, true);
        byte[] bytes = Files.readAllBytes(file);
        bytes = Arrays.copyOf(bytes, tailOffset(bytes) + 3);
        Files.write(file, bytes);
        assertEquals(NO_EOF, OfflineCycleFileTruncator.truncate(file, true));
        assertArrayEquals(bytes, Files.readAllBytes(file));
    }

    @Test
    public void unsupportedFormatIsRefusedWithoutMutation() throws IOException {
        Path file = fixture(2, true);
        byte[] bytes = Files.readAllBytes(file);
        assertThrows(IOException.class, () -> OfflineCycleFileTruncator.truncate(file, true));
        assertArrayEquals(bytes, Files.readAllBytes(file));
    }

    @Test
    public void corruptLengthIsRefusedWithoutMutation() throws IOException {
        Path file = fixture(1, false);
        byte[] bytes = Files.readAllBytes(file);
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).putInt(tailOffset(bytes), 100000);
        Files.write(file, bytes);
        assertThrows(IOException.class, () -> OfflineCycleFileTruncator.truncate(file, true));
        assertArrayEquals(bytes, Files.readAllBytes(file));
    }

    @Test
    public void missingFileIsNotCreated() {
        Path file = temporary.getRoot().toPath().resolve("missing.cq4");
        assertThrows(IOException.class, () -> OfflineCycleFileTruncator.truncate(file, true));
        assertFalse(Files.exists(file));
    }

    @Test
    public void realRolledQueueReplaysAfterOfflineTrim() throws IOException {
        Path directory = temporary.newFolder().toPath();
        SetTimeProvider clock = new SetTimeProvider(86400000L);
        try (SingleChronicleQueue queue = builder(directory, clock).build()) {
            ExcerptAppender appender = queue.createAppender();
            appender.writeText("first");
            clock.advanceMillis(86400000L);
            appender.writeText("second");
        }
        net.openhft.chronicle.core.io.BackgroundResourceReleaser.releasePendingResources();
        Path[] cycles;
        try (java.util.stream.Stream<Path> files = Files.list(directory)) {
            cycles = files.filter(f -> f.toString().endsWith(".cq4")).sorted().toArray(Path[]::new);
        }
        assertEquals(2, cycles.length);
        assertEquals(TRUNCATED, OfflineCycleFileTruncator.truncate(cycles[0], true));
        assertEquals(NO_EOF, OfflineCycleFileTruncator.truncate(cycles[1], true));
        try (SingleChronicleQueue queue = builder(directory, clock).readOnly(true).build();
             ExcerptTailer tailer = queue.createTailer()) {
            assertEquals("first", tailer.readText());
            assertEquals("second", tailer.readText());
            assertNull(tailer.readText());
        }
    }

    @Test
    public void bufferedInspectionPreservesRecordsAcrossReadAheadBoundaries() throws IOException {
        Path directory = temporary.newFolder().toPath();
        SetTimeProvider clock = new SetTimeProvider(86400000L);
        String prefix = "payload across read-ahead windows: ";
        try (SingleChronicleQueue queue = builder(directory, clock).rollCycle(TestRollCycles.TEST8_DAILY).build()) {
            ExcerptAppender appender = queue.createAppender();
            for (int i = 0; i < 4096; i++)
                appender.writeText(prefix + i);
            clock.advanceMillis(86400000L);
            appender.writeText("next cycle");
        }
        net.openhft.chronicle.core.io.BackgroundResourceReleaser.releasePendingResources();
        Path first;
        try (java.util.stream.Stream<Path> files = Files.list(directory)) {
            first = files.filter(f -> f.toString().endsWith(".cq4")).sorted().findFirst().get();
        }
        long before = Files.size(first);
        assertEquals(TRUNCATED, OfflineCycleFileTruncator.truncate(first, true));
        long after = Files.size(first);
        assertTrue(after > 2 * 64 * 1024);
        assertTrue(after < before);
        try (SingleChronicleQueue queue = builder(directory, clock).rollCycle(TestRollCycles.TEST8_DAILY).readOnly(true).build();
             ExcerptTailer tailer = queue.createTailer()) {
            for (int i = 0; i < 4096; i++)
                assertEquals(prefix + i, tailer.readText());
            assertEquals("next cycle", tailer.readText());
            assertNull(tailer.readText());
        }
    }

    @Test
    public void noEofEmitsDebugReason() throws IOException {
        net.openhft.chronicle.core.onoes.ExceptionHandler previous = net.openhft.chronicle.core.Jvm.debug();
        java.util.List<String> messages = new java.util.ArrayList<>();
        try {
            net.openhft.chronicle.core.Jvm.setDebugExceptionHandler((type, message, thrown) -> {
                if (type.getName().equals(OfflineCycleFileTruncator.class.getName()))
                    messages.add(message);
            });
            assertEquals(NO_EOF, OfflineCycleFileTruncator.truncate(fixture(1, false), false));
            assertEquals(1, messages.size());
            assertTrue(messages.get(0).contains("truncation is deferred until EOF is present"));
        } finally {
            net.openhft.chronicle.core.Jvm.setDebugExceptionHandler(previous);
        }
    }

    @Test(timeout = 30000)
    public void liveWriterInAnotherProcessCanContinueAfterDeferral() throws Exception {
        Path directory = temporary.newFolder().toPath();
        java.util.List<String> command = new java.util.ArrayList<>();
        command.add(java.nio.file.Paths.get(System.getProperty("java.home"), "bin", "java").toString());
        java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
                .filter(a -> a.startsWith("--add-") || a.startsWith("--enable-"))
                .forEach(command::add);
        command.add("-Dchronicle.analytics.disable=true");
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(TruncationWriterProcess.class.getName());
        command.add(directory.toString());
        Process process = new ProcessBuilder(command).redirectError(directory.resolve("writer.log").toFile()).start();
        try (java.io.BufferedReader input = new java.io.BufferedReader(new java.io.InputStreamReader(
                process.getInputStream(), java.nio.charset.StandardCharsets.UTF_8));
             java.io.OutputStream output = process.getOutputStream()) {
            assertEquals("READY", input.readLine());
            Path file;
            try (java.util.stream.Stream<Path> paths = Files.list(directory)) {
                file = paths.filter(f -> f.toString().endsWith(".cq4")).findFirst().get();
            }
            byte[] before = Files.readAllBytes(file);
            assertThrows(IOException.class, () -> OfflineCycleFileTruncator.truncate(file, false));
            assertArrayEquals(before, Files.readAllBytes(file));
            output.write(1);
            output.flush();
            assertEquals("APPENDED", input.readLine());
            assertTrue(process.waitFor(10, java.util.concurrent.TimeUnit.SECONDS));
            assertEquals(0, process.exitValue());
        } finally {
            process.destroyForcibly();
            process.waitFor(10, java.util.concurrent.TimeUnit.SECONDS);
        }
        try (SingleChronicleQueue queue = builder(directory, new SetTimeProvider(86400000L)).readOnly(true).build();
             ExcerptTailer tailer = queue.createTailer()) {
            assertEquals("first", tailer.readText());
            assertEquals("second", tailer.readText());
            assertNull(tailer.readText());
        }
    }

    @Test
    public void dataAfterEofIsNeverDiscarded() throws IOException {
        Path file = fixture(1, true);
        byte[] before = Files.readAllBytes(file);
        before[before.length - 1] = 42;
        Files.write(file, before);
        assertEquals(SKIPPED_NONZERO_TAIL, OfflineCycleFileTruncator.truncate(file, false));
        assertEquals(SKIPPED_NONZERO_TAIL, OfflineCycleFileTruncator.truncate(file, true));
        assertArrayEquals(before, Files.readAllBytes(file));
    }

    @Test
    public void scalarWritePositionIsSupported() throws IOException {
        assertEquals(TRUNCATED, OfflineCycleFileTruncator.truncate(fixture(1, true, true), true));
    }

    @Test
    public void missingOrWrongAnnotationIsRefused() throws IOException {
        for (byte value : new byte[]{0, 'X'}) {
            Path file = fixture(1, true);
            byte[] data = Files.readAllBytes(file);
            data[tailOffset(data) + 4] = value;
            Files.write(file, data);
            assertEquals(NO_EOF, OfflineCycleFileTruncator.truncate(file, true));
            assertArrayEquals(data, Files.readAllBytes(file));
        }
    }

    @Test
    public void replacedFileIsRefusedEvenAtSameSize() throws IOException {
        Path file = fixture(1, true);
        CycleFileIdentity identity = CycleFileIdentity.capture(file);
        byte[] data = Files.readAllBytes(file);
        Files.move(file, file.resolveSibling(file.getFileName() + ".original"));
        Files.write(file, data);
        assertThrows(IOException.class, () -> OfflineCycleFileTruncator.truncate(file, true, identity));
        assertArrayEquals(data, Files.readAllBytes(file));
    }

    @Test
    public void parentSymlinksAndOpenHandlesAreRefused() throws IOException {
        Path directory = temporary.newFolder().toPath();
        Path original = fixture(1, true);
        Path file = directory.resolve("roll.cq4");
        Files.move(original, file);
        Path alias = temporary.getRoot().toPath().resolve("alias");
        Files.createSymbolicLink(alias, directory);
        assertThrows(IOException.class, () -> OfflineCycleFileTruncator.truncate(alias.resolve("roll.cq4"), true));
        try (FileChannel open = FileChannel.open(file)) {
            assertTrue(open.isOpen());
            assertThrows(IOException.class, () -> OfflineCycleFileTruncator.truncate(file, false));
            assertThrows(IOException.class, () -> OfflineCycleFileTruncator.truncate(file, true));
        }
    }

    @Test
    public void hardLinkBackupAndLateHardLinkAreRefused() throws IOException {
        Path file = fixture(1, true);
        byte[] before = Files.readAllBytes(file);
        CycleFileIdentity expected = CycleFileIdentity.capture(file);
        Path alias = file.resolveSibling("backup.bin");
        Files.createLink(alias, file);
        IOException error = assertThrows(IOException.class,
                () -> OfflineCycleFileTruncator.truncate(file, true));
        assertTrue(error.getMessage(), error.getMessage().startsWith("REFUSED_HARD_LINKS"));
        assertThrows(IOException.class, () -> expected.check(file));
        assertArrayEquals(before, Files.readAllBytes(alias));
        assertArrayEquals(before, Files.readAllBytes(file));
    }

    @Test
    public void lowLevelTruncatorHasNoPublicEntrypoint() {
        assertFalse(java.lang.reflect.Modifier.isPublic(OfflineCycleFileTruncator.class.getModifiers()));
        assertThrows(NoSuchMethodException.class,
                () -> OfflineCycleFileTruncator.class.getMethod("main", String[].class));
    }

    private SingleChronicleQueueBuilder builder(Path directory, SetTimeProvider clock) {
        return SingleChronicleQueueBuilder.binary(directory.toFile()).testBlockSize()
                .rollCycle(TestRollCycles.TEST_DAILY).timeProvider(clock);
    }

    private Path fixture(int format, boolean eof) throws IOException {
        return fixture(format, eof, false);
    }

    private Path fixture(int format, boolean eof, boolean scalar) throws IOException {
        Bytes<?> bytes = Bytes.allocateElasticOnHeap();
        byte[] header;
        try {
            new BinaryWire(bytes).writeEventName("header").typePrefix("SCQStore")
                    .marshallable(w -> {
                        try (net.openhft.chronicle.bytes.ref.BinaryTwoLongReference position =
                                     new net.openhft.chronicle.bytes.ref.BinaryTwoLongReference()) {
                            if (scalar)
                                w.write("writePosition").int64(0);
                            else
                                w.write("writePosition").int128forBinding(0, 0, position);
                            w.write("indexing").marshallable(index -> index.write("indexCount").int32(8));
                            w.write("dataFormat").int32(format);
                        }
                    });
            header = bytes.toByteArray();
        } finally {
            bytes.releaseLast();
        }
        ByteBuffer data = ByteBuffer.allocate(4096).order(ByteOrder.LITTLE_ENDIAN);
        data.putInt(Wires.META_DATA | header.length).put(header);
        data.position((data.position() + 3) & ~3);
        data.putInt(4).putInt(Wires.END_OF_DATA); // EOF-shaped payload, not a marker
        data.putInt(eof ? Wires.END_OF_DATA : 0);
        if (eof)
            data.put(new byte[]{(byte) 0xc3, 'E', 'O', 'F'});
        Path file = temporary.newFile().toPath();
        Files.write(file, data.array());
        return file;
    }

    private int tailOffset(byte[] bytes) {
        int header = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
        return ((4 + Wires.lengthOf(header) + 3) & ~3) + 8;
    }
}
