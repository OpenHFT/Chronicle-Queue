/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.util;

import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.Jvm;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

    @Test
    public void warmupDrainsDeferredResourceReleases() throws Exception {
        runChild("deferred");
    }

    private void runChild(String mode) throws Exception {
        Path scratch = temporary.getRoot().toPath().resolve("scratch");
        Path output = temporary.getRoot().toPath().resolve("child.log");
        List<String> command = new ArrayList<>();
        command.add(Paths.get(System.getProperty("java.home"), "bin", "java").toString());
        command.addAll(ManagementFactory.getRuntimeMXBean().getInputArguments());
        if ("deferred".equals(mode))
            command.add("-Dbackground.releaser.thread=false");
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
                AtomicBoolean released = new AtomicBoolean();
                if ("deferred".equals(args[0]))
                    BackgroundResourceReleaser.run(() -> released.set(true));
                Path unrelated = scratch.resolve("native-library-fixture.dll");
                Files.write(unrelated, new byte[]{1, 2, 3});
                // A polling WatchService can miss a directory created and deleted between polls.
                // The child process captures the synchronous creation diagnostic instead.
                AtomicInteger directories = new AtomicInteger();
                Jvm.setDebugExceptionHandler((logger, message, failure) -> {
                    if (IndexWarmup.class.getName().equals(logger.getName())
                            && message.startsWith("Warming Queue indexing in "))
                        directories.incrementAndGet();
                });
                {
                    Class.forName(ToolsUtil.class.getName());
                    ToolsUtil.warnIfResourceTracing();
                    assertNoWarmupFiles(scratch);
                    assertEquals("Ordinary utility use started warm-up", 0, directories.get());
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
                        if ("deferred".equals(args[0]))
                            assertTrue("Deferred releases remain after warm-up", released.get());
                        assertNoWarmupFiles(scratch);
                        assertArrayEquals(new byte[]{1, 2, 3}, Files.readAllBytes(unrelated));
                        assertEquals("Warm-up must create exactly one queue", 1, directories.get());
                    } finally {
                        threads.shutdownNow();
                        assertTrue(threads.awaitTermination(5, TimeUnit.SECONDS));
                    }
                }
            }
            System.out.println("warm-up checks passed");
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
