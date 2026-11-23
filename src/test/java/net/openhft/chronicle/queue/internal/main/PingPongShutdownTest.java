/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.main;

import net.openhft.chronicle.queue.QueueTestCommon;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class PingPongShutdownTest extends QueueTestCommon {
    public static class Child {
        public static void main(String[] args) throws Exception {
            InternalPingPongMain.runtime = Integer.parseInt(args[0]);
            InternalPingPongMain.basePath = args[1];
            InternalPingPongMain.pingPong(64);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (hasReader() && System.nanoTime() < deadline)
                Thread.sleep(10);
            if (hasReader())
                throw new AssertionError("Ping-pong reader remains alive after shutdown");
            File[] files = new File(args[1]).listFiles();
            if (files != null && files.length != 0)
                throw new AssertionError("Queue files remain after shutdown");
        }

        private static boolean hasReader() {
            for (Map.Entry<Thread, StackTraceElement[]> e : Thread.getAllStackTraces().entrySet()) {
                if (e.getKey() == Thread.currentThread())
                    continue;
                for (StackTraceElement frame : e.getValue())
                    if (frame.getClassName().equals(InternalPingPongMain.class.getName()))
                        return true;
            }
            return false;
        }
    }

    private void runChild(int duration) throws Exception {
        runChild(duration, true);
    }

    private void runChild(int duration, boolean backgroundReleaserThread) throws Exception {
        File dir = getTmpDir();
        Files.createDirectories(dir.toPath());
        Path log = Files.createTempFile("ping-pong-probe-", ".log");
        List<String> command = new ArrayList<>();
        command.add(new File(System.getProperty("java.home"), "bin/java").getPath());
        for (String arg : java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments())
            // Shutdown is checked in a child JVM; include its execution in the coverage report.
            if (arg.startsWith("--add-opens") || arg.startsWith("--add-exports")
                    || (arg.startsWith("-javaagent:") && arg.contains("org.jacoco.agent")))
                command.add(arg);
        Collections.addAll(command, "-Xms64m", "-Xmx512m",
                "-Dbackground.releaser.thread=" + backgroundReleaserThread,
                "-ea", "-cp", System.getProperty("java.class.path"), Child.class.getName(), Integer.toString(duration), dir.getAbsolutePath());
        Process child = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            boolean finished = child.waitFor(25, TimeUnit.SECONDS);
            if (!finished) {
                child.destroyForcibly();
                child.waitFor(5, TimeUnit.SECONDS);
            }
            String output = new String(Files.readAllBytes(log), StandardCharsets.UTF_8);
            assertTrue(output, finished);
            assertEquals(output, 0, child.exitValue());
        } finally {
            child.destroyForcibly();
            child.waitFor(5, TimeUnit.SECONDS);
            child.getInputStream().close();
            child.getErrorStream().close();
            child.getOutputStream().close();
            Files.deleteIfExists(log);
        }
    }

    @Test
    public void zeroDurationStopsIdleReader() throws Exception {
        runChild(0);
    }

    @Test
    public void ordinaryRunStopsReaderBeforeDeletingFiles() throws Exception {
        runChild(1);
    }

    @Test
    public void ordinaryRunDrainsDeferredReleasesBeforeDeletingFiles() throws Exception {
        // Without a releaser thread, cleanup must explicitly finish queued unmapping.
        runChild(1, false);
    }

    @Test
    public void interruptedShutdownWaitsForReaderAndPreservesInterrupt() throws Exception {
        boolean wasRunning = InternalPingPongMain.running.getAndSet(true);
        Thread reader = new Thread(() -> {
            while (InternalPingPongMain.running.get())
                Thread.yield();
        });
        reader.start();
        try {
            Thread.currentThread().interrupt();
            InternalPingPongMain.stopReader(reader);
            assertFalse(reader.isAlive());
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
            InternalPingPongMain.running.set(false);
            reader.join(5000);
            InternalPingPongMain.running.set(wasRunning);
        }
    }
}
