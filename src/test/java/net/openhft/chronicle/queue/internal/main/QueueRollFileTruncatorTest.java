/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.main;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class QueueRollFileTruncatorTest {
    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void recursivePreviewPreservesAllBytes() throws Exception {
        Path root = temporary.newFolder().toPath();
        List<Path> first = createQueue(root.resolve("first"));
        List<Path> second = createQueue(root.resolve("deep/second"));
        byte[] beforeA = Files.readAllBytes(first.get(0));
        byte[] beforeB = Files.readAllBytes(second.get(0));
        String output = scan(root, false);
        assertTrue(output, output.contains(first.get(0).toAbsolutePath() + " OFFLINE_CONFIRMATION_REQUIRED"));
        assertTrue(output, output.contains(second.get(2).toAbsolutePath() + " SKIPPED_NEWEST"));
        assertArrayEquals(beforeA, Files.readAllBytes(first.get(0)));
        assertArrayEquals(beforeB, Files.readAllBytes(second.get(0)));
    }

    @Test
    public void recursiveApplySkipsNewestInEveryQueueAndReplays() throws Exception {
        Path root = temporary.newFolder().toPath();
        List<Path> first = createQueue(root.resolve("first"));
        List<Path> second = createQueue(root.resolve("deep/second"));
        byte[] newestA = Files.readAllBytes(first.get(2));
        byte[] newestB = Files.readAllBytes(second.get(2));
        String output = scan(root, true);
        assertTrue(output, output.contains(first.get(0).toAbsolutePath() + " TRUNCATED"));
        assertTrue(output, output.contains(second.get(1).toAbsolutePath() + " TRUNCATED"));
        assertArrayEquals(newestA, Files.readAllBytes(first.get(2)));
        assertArrayEquals(newestB, Files.readAllBytes(second.get(2)));
        replay(root.resolve("first"));
        replay(root.resolve("deep/second"));
        assertTrue(scan(root, true).contains("SKIPPED_SIZE"));
    }

    @Test
    public void nonAlignedAndNoMetadataFilesAreUntouched() throws Exception {
        Path root = temporary.newFolder().toPath();
        List<Path> files = createQueue(root.resolve("queue"));
        try (RandomAccessFile file = new RandomAccessFile(files.get(0).toFile(), "rw")) {
            file.setLength(file.length() + 1);
        }
        byte[] before = Files.readAllBytes(files.get(0));
        Path unknown = Files.createDirectories(root.resolve("unknown")).resolve("19700101.cq4");
        byte[] sentinel = new byte[65536];
        Files.write(unknown, sentinel);
        String output = scan(root, true);
        assertTrue(output, output.contains(files.get(0).toAbsolutePath() + " SKIPPED_SIZE"));
        assertTrue(output, output.contains(unknown.getParent().toAbsolutePath() + " ERROR IOException: SKIPPED_NO_METADATA"));
        assertArrayEquals(before, Files.readAllBytes(files.get(0)));
        assertArrayEquals(sentinel, Files.readAllBytes(unknown));
    }

    @Test
    public void directorySymlinkIsNotTraversed() throws Exception {
        Path root = temporary.newFolder().toPath();
        Path outside = temporary.newFolder().toPath();
        List<Path> files = createQueue(outside);
        byte[] before = Files.readAllBytes(files.get(0));
        Files.createSymbolicLink(root.resolve("alias"), outside);
        assertTrue(scan(root, true).contains("SUMMARY directories=0 errors=1 status=INCOMPLETE"));
        assertArrayEquals(before, Files.readAllBytes(files.get(0)));
    }

    @Test
    public void maintenanceRefusesAnOpenQueueBeforeMutation() throws Exception {
        Path root = temporary.newFolder().toPath();
        List<Path> files = createQueue(root);
        byte[] before = Files.readAllBytes(files.get(0));
        try (SingleChronicleQueue writer = SingleChronicleQueueBuilder.binary(root.toFile())
                .testBlockSize().rollCycle(TestRollCycles.TEST_DAILY).build()) {
            writer.createAppender();
            assertTrue(scan(root, true).contains("REFUSED_FILE_STATE_OPEN"));
            assertArrayEquals(before, Files.readAllBytes(files.get(0)));
        }
    }

    @Test
    public void invalidDateCannotStealNewestAndLaterQueuesAreVisited() throws Exception {
        Path root = temporary.newFolder().toPath();
        List<Path> first = createQueue(root.resolve("a-good"));
        List<Path> bad = createQueue(root.resolve("b-invalid"));
        List<Path> last = createQueue(root.resolve("c-good"));
        byte[] newest = Files.readAllBytes(bad.get(2));
        byte[] older = Files.readAllBytes(bad.get(0));
        Files.write(root.resolve("b-invalid/19700231T1.cq4"), new byte[65536]);
        String output = scan(root, true);
        assertTrue(output, output.contains("Noncanonical roll name"));
        assertTrue(output, output.contains(first.get(0).toAbsolutePath() + " TRUNCATED"));
        assertTrue(output, output.contains(last.get(0).toAbsolutePath() + " TRUNCATED"));
        assertTrue(output, output.contains("status=INCOMPLETE"));
        assertArrayEquals(newest, Files.readAllBytes(bad.get(2)));
        assertArrayEquals(older, Files.readAllBytes(bad.get(0)));
    }

    @Test
    public void symlinkRootsAndParentsAreRefused() throws Exception {
        Path real = temporary.newFolder().toPath();
        createQueue(real.resolve("queue"));
        Path alias = temporary.getRoot().toPath().resolve("alias");
        Files.createSymbolicLink(alias, real);
        assertThrows(java.io.IOException.class, () -> scan(alias, true));
        assertThrows(java.io.IOException.class, () -> scan(alias.resolve("queue"), true));
    }

    @Test
    public void unrelatedSymlinkRollDoesNotCreateAQueue() throws Exception {
        Path root = temporary.newFolder().toPath();
        Files.createSymbolicLink(root.resolve("unused.cq4"), root.resolve("missing"));
        assertTrue(scan(root, true).contains("directories=0 errors=1 status=INCOMPLETE"));
    }

    @Test(timeout = 30000)
    public void publicCliPreviewsAndAppliesInSeparateProcesses() throws Exception {
        Path root = temporary.newFolder().toPath();
        List<Path> files = createQueue(root.resolve("queue"));
        byte[] before = Files.readAllBytes(files.get(0));
        assertEquals(0, cli(root, false));
        assertArrayEquals(before, Files.readAllBytes(files.get(0)));
        assertEquals(0, cli(root, true));
        assertTrue(Files.size(files.get(0)) < before.length);
        replay(root.resolve("queue"));
    }

    @Test(timeout = 30000)
    public void publicCliReturnsFailureAfterContinuingPastBadQueue() throws Exception {
        Path root = temporary.newFolder().toPath();
        createQueue(root.resolve("a-bad"));
        Files.write(root.resolve("a-bad/19700231T1.cq4"), new byte[65536]);
        List<Path> later = createQueue(root.resolve("z-good"));
        long before = Files.size(later.get(0));
        assertNotEquals(0, cli(root, true));
        assertTrue(Files.size(later.get(0)) < before);
    }

    @Test(timeout = 30000)
    public void publicCliRefusesHardLinksAndNonzeroResidueButContinues() throws Exception {
        Path root = temporary.newFolder().toPath();
        List<Path> linked = createQueue(root.resolve("a-linked"));
        byte[] before = Files.readAllBytes(linked.get(0));
        Path alias = temporary.getRoot().toPath().resolve("backup.bin");
        Files.createLink(alias, linked.get(0));
        List<Path> residue = createQueue(root.resolve("b-residue"));
        byte[] dirty = Files.readAllBytes(residue.get(0));
        dirty[dirty.length - 1] = 1;
        Files.write(residue.get(0), dirty);
        List<Path> good = createQueue(root.resolve("z-good"));
        long goodSize = Files.size(good.get(0));
        assertNotEquals(0, cli(root, true));
        String output = new String(Files.readAllBytes(root.resolve("cli.log")), StandardCharsets.UTF_8);
        assertTrue(output, output.contains("REFUSED_HARD_LINKS"));
        assertTrue(output, output.contains("SKIPPED_NONZERO_TAIL"));
        assertTrue(output, output.contains("status=INCOMPLETE"));
        assertArrayEquals(before, Files.readAllBytes(alias));
        assertArrayEquals(dirty, Files.readAllBytes(residue.get(0)));
        assertTrue(Files.size(good.get(0)) < goodSize);
    }

    @Test
    public void emptyFutureFileCannotReplaceNewestGenuineRoll() throws Exception {
        Path root = temporary.newFolder().toPath();
        List<Path> files = createQueue(root);
        // Make mistaken eligibility observable by putting EOF on the genuine newest roll.
        Files.copy(files.get(0), files.get(2), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        byte[] newest = Files.readAllBytes(files.get(2));
        Path future = Files.createFile(root.resolve("19700105T1.cq4"));
        String output = scan(root, true);
        assertTrue(output, output.contains(files.get(2).toAbsolutePath() + " SKIPPED_NEWEST"));
        assertTrue(output, output.contains(future.toAbsolutePath() + " SKIPPED_SIZE"));
        assertTrue(output, output.contains(files.get(0).toAbsolutePath() + " TRUNCATED"));
        assertArrayEquals(newest, Files.readAllBytes(files.get(2)));
        assertEquals(0, Files.size(future));
    }

    @Test
    public void malformedFutureHeaderRefusesQueueBeforeAnyMutation() throws Exception {
        Path root = temporary.newFolder().toPath();
        List<Path> bad = createQueue(root.resolve("a-bad"));
        Files.copy(bad.get(0), bad.get(2), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        byte[] before = Files.readAllBytes(bad.get(0));
        byte[] newest = Files.readAllBytes(bad.get(2));
        Files.write(root.resolve("a-bad/19700105T1.cq4"), new byte[4096]);
        List<Path> good = createQueue(root.resolve("z-good"));
        String output = scan(root, true);
        assertTrue(output, output.contains("Expected a complete Queue store header"));
        assertTrue(output, output.contains("status=INCOMPLETE"));
        assertTrue(output, output.contains(good.get(0).toAbsolutePath() + " TRUNCATED"));
        assertArrayEquals(before, Files.readAllBytes(bad.get(0)));
        assertArrayEquals(newest, Files.readAllBytes(bad.get(2)));
    }

    @Test
    public void newestHeaderIsValidatedEvenThoughItWillNotBeTrimmed() throws Exception {
        Path root = temporary.newFolder().toPath();
        List<Path> files = createQueue(root);
        byte[] before = Files.readAllBytes(files.get(0));
        Files.write(files.get(2), new byte[4096]);
        String output = scan(root, true);
        assertTrue(output, output.contains("Expected a complete Queue store header"));
        assertArrayEquals(before, Files.readAllBytes(files.get(0)));
        assertArrayEquals(new byte[4096], Files.readAllBytes(files.get(2)));
    }

    private int cli(Path root, boolean apply) throws Exception {
        java.util.List<String> command = new java.util.ArrayList<>();
        command.add(java.nio.file.Paths.get(System.getProperty("java.home"), "bin", "java").toString());
        java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
                .filter(a -> a.startsWith("--add-") || a.startsWith("--enable-"))
                .forEach(command::add);
        command.add("-Dchronicle.analytics.disable=true");
        command.add("-cp"); command.add(System.getProperty("java.class.path"));
        command.add("net.openhft.chronicle.queue.main.TruncateRollFilesMain");
        command.add(root.toString());
        command.add("--min-age-seconds=0");
        if (apply) command.add("--offline");
        Process child = new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(root.resolve("cli.log").toFile()).start();
        try {
            assertTrue(child.waitFor(20, java.util.concurrent.TimeUnit.SECONDS));
            if (child.exitValue() != 0)
                System.err.println(new String(Files.readAllBytes(root.resolve("cli.log")), StandardCharsets.UTF_8));
            return child.exitValue();
        } finally {
            child.destroyForcibly();
            child.waitFor(5, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    private String scan(Path root, boolean offline) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (PrintStream output = new PrintStream(bytes, false, "UTF-8")) {
            InternalTruncateRollFilesMain.scanTree(root, offline, output, 0);
        }
        return new String(bytes.toByteArray(), StandardCharsets.UTF_8);
    }

    private List<Path> createQueue(Path directory) throws Exception {
        Files.createDirectories(directory);
        SetTimeProvider clock = new SetTimeProvider(86400000L);
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory.toFile())
                .testBlockSize().rollCycle(TestRollCycles.TEST_DAILY).timeProvider(clock).build()) {
            ExcerptAppender appender = queue.createAppender();
            for (int i = 0; i < 3; i++) {
                appender.writeText("record-" + i);
                clock.advanceMillis(86400000L);
            }
        }
        net.openhft.chronicle.core.io.BackgroundResourceReleaser.releasePendingResources();
        List<Path> files;
        try (Stream<Path> paths = Files.list(directory)) {
            files = paths.filter(p -> p.toString().endsWith(".cq4")).sorted().collect(Collectors.toList());
        }
        for (Path path : files) {
            try (RandomAccessFile file = new RandomAccessFile(path.toFile(), "rw")) {
                file.setLength(((file.length() + 65535) / 65536) * 65536);
            }
        }
        return files;
    }

    private void replay(Path directory) {
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory.toFile()).readOnly(true).build();
             ExcerptTailer tailer = queue.createTailer()) {
            for (int i = 0; i < 3; i++)
                assertEquals("record-" + i, tailer.readText());
            assertNull(tailer.readText());
        }
    }
}
