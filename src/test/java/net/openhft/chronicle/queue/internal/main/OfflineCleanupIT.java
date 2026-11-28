/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.main;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.junit.Assert.*;
import static net.openhft.chronicle.queue.internal.main.QueueRollFileTruncatorTest.createQueue;
import static net.openhft.chronicle.queue.internal.main.QueueRollFileTruncatorTest.replay;

/** Real process inspection: run explicitly with -Poffline-cleanup-integration. */
public class OfflineCleanupIT extends net.openhft.chronicle.queue.QueueTestCommon {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @org.junit.BeforeClass
    public static void requireProcessVisibility() throws Exception {
        assertTrue("This profile requires Linux /proc access", net.openhft.chronicle.core.OS.isLinux());
        Path probe = Files.createTempFile("cleanup-preflight", ".cq4");
        try {
            OfflineFileUsers.requireClosed(probe);
        } catch (IOException e) {
            throw new AssertionError("Integration profile requires visibility of every process fd/maps; "
                    + "use an isolated Linux test host with the required permissions", e);
        } finally {
            Files.delete(probe);
        }
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
        try (SingleChronicleQueue queue = OfflineCycleFileTruncatorTest.builder(directory, new SetTimeProvider(86400000L)).readOnly(true).build();
             ExcerptTailer tailer = queue.createTailer()) {
            assertEquals("first", tailer.readText());
            assertEquals("second", tailer.readText());
            assertNull(tailer.readText());
        }
    }

    private static int cli(Path root, boolean apply) throws Exception {
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

}
