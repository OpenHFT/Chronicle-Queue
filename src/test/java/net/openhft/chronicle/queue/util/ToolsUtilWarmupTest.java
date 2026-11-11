/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.util;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class ToolsUtilWarmupTest {
    @Rule
    public TemporaryFolder temporary = new TemporaryFolder(new File("target"));

    @Test
    public void lazyConcurrentWarmupRunsOnceAndCleansUp() throws Exception {
        runChild("success");
    }

    @Test
    public void initializationFailureIsStableWithoutRetry() throws Exception {
        runChild("failure");
    }

    private void runChild(String mode) throws Exception {
        Path scratch = temporary.getRoot().toPath().resolve("scratch");
        Path output = temporary.getRoot().toPath().resolve("child.log");
        List<String> command = new ArrayList<>();
        command.add(Paths.get(System.getProperty("java.home"), "bin", "java").toString());
        command.addAll(ManagementFactory.getRuntimeMXBean().getInputArguments());
        command.add("-Djava.io.tmpdir=" + scratch.toAbsolutePath());
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(Child.class.getName());
        command.add(mode);
        Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(output.toFile()).start();
        try {
            assertTrue("Warm-up child timed out", process.waitFor(60, TimeUnit.SECONDS));
            String text = new String(Files.readAllBytes(output), StandardCharsets.UTF_8);
            assertEquals(text, 0, process.exitValue());
            assertTrue(text, text.contains("warm-up checks passed"));
        } finally {
            process.destroyForcibly();
            assertTrue("Child did not stop", process.waitFor(5, TimeUnit.SECONDS));
        }
    }

    public static class Child {
        public static void main(String[] args) throws Exception {
            Path scratch = Paths.get(System.getProperty("java.io.tmpdir"));
            if ("failure".equals(args[0])) {
                IllegalStateException first = assertThrows(IllegalStateException.class, ToolsUtil::warmup);
                Files.createDirectories(scratch);
                IllegalStateException second = assertThrows(IllegalStateException.class, ToolsUtil::warmup);
                assertSame(first.getCause(), second.getCause());
                assertNoWarmupFiles(scratch);
            } else {
                Files.createDirectories(scratch);
                Path unrelated = scratch.resolve("native-library-fixture.dll");
                Files.write(unrelated, new byte[]{1, 2, 3});
                try (WatchService watcher = scratch.getFileSystem().newWatchService()) {
                    scratch.register(watcher, StandardWatchEventKinds.ENTRY_CREATE);
                    Class.forName(ToolsUtil.class.getName());
                    ToolsUtil.warnIfResourceTracing();
                    assertNoWarmupFiles(scratch);
                    WatchKey initial = watcher.poll();
                    if (initial != null) {
                        assertEquals("Ordinary utility use started warm-up", 0, warmupDirectories(initial));
                        initial.reset();
                    }
                    ExecutorService threads = Executors.newFixedThreadPool(8);
                    CountDownLatch start = new CountDownLatch(1);
                    List<Future<?>> calls = new ArrayList<>();
                    try {
                        for (int i = 0; i < 8; i++) {
                            calls.add(threads.submit(() -> {
                                start.await();
                                ToolsUtil.warmup();
                                assertNoWarmupFiles(scratch);
                                return null;
                            }));
                        }
                        start.countDown();
                        for (Future<?> call : calls)
                            call.get(45, TimeUnit.SECONDS);
                        ToolsUtil.warmup();
                        assertNoWarmupFiles(scratch);
                        assertArrayEquals(new byte[]{1, 2, 3}, Files.readAllBytes(unrelated));
                        WatchKey key = watcher.poll(5, TimeUnit.SECONDS);
                        assertNotNull("No warm-up directory created", key);
                        long directories = warmupDirectories(key);
                        assertEquals("Warm-up must create exactly one queue", 1, directories);
                        key.reset();
                        WatchKey additional = watcher.poll();
                        if (additional != null)
                            assertEquals("Unexpected additional warm-up", 0, warmupDirectories(additional));
                    } finally {
                        threads.shutdownNow();
                        assertTrue(threads.awaitTermination(5, TimeUnit.SECONDS));
                    }
                }
            }
            System.out.println("warm-up checks passed");
        }

        private static long warmupDirectories(WatchKey key) {
            return key.pollEvents().stream().filter(event ->
                    event.kind() == StandardWatchEventKinds.ENTRY_CREATE
                            && event.context().toString().startsWith("chronicle-queue-index-warmup-")).count();
        }

        private static void assertNoWarmupFiles(Path directory) throws Exception {
            try (Stream<Path> files = Files.list(directory)) {
                // Native libraries may create their own files in java.io.tmpdir, particularly on Windows.
                // Only this utility's owned directory prefix establishes its cleanup contract.
                assertEquals("Warm-up files remain", 0, files.filter(path ->
                        path.getFileName().toString().startsWith("chronicle-queue-index-warmup-")).count());
            }
        }
    }
}
