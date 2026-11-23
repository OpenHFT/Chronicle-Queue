/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.main;

import net.openhft.chronicle.core.io.Closeable;
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
        public static void main(String[] args) {
            InternalPingPongMain.runtime = Integer.parseInt(args[0]);
            InternalPingPongMain.basePath = args[1];
            InternalPingPongMain.pingPong(64);
            for (StackTraceElement[] stack : Thread.getAllStackTraces().values()) {
                for (StackTraceElement frame : stack)
                    assertNotEquals("Reader still running", InternalPingPongMain.class.getName(), frame.getClassName());
            }
            assertArrayEquals("Queue files remain after shutdown", new String[0], new File(args[1]).list());
        }
    }

    private void runChild(int duration, boolean backgroundReleaserThread) throws Exception {
        Path dir = getTmpDir().toPath();
        Path queue = Files.createDirectories(dir.resolve("queue"));
        Path log = dir.resolve("child.log");
        List<String> command = new ArrayList<>();
        command.add(new File(System.getProperty("java.home"), "bin/java").getPath());
        for (String arg : java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments())
            // Shutdown is checked in a child JVM; include its execution in the coverage report.
            if (arg.startsWith("--add-opens") || arg.startsWith("--add-exports")
                    || (arg.startsWith("-javaagent:") && arg.contains("org.jacoco.agent")))
                command.add(arg);
        Collections.addAll(command, "-Xms64m", "-Xmx512m",
                "-Dbackground.releaser.thread=" + backgroundReleaserThread,
                "-ea", "-cp", System.getProperty("java.class.path"), Child.class.getName(), Integer.toString(duration), queue.toString());
        Process child = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        boolean finished;
        try {
            finished = child.waitFor(25, TimeUnit.SECONDS);
        } finally {
            if (child.isAlive()) {
                child.destroyForcibly();
                assertTrue("Ping-pong child survived termination", child.waitFor(5, TimeUnit.SECONDS));
            }
            Closeable.closeQuietly(child.getInputStream(), child.getErrorStream(), child.getOutputStream());
        }
        String output = new String(Files.readAllBytes(log), StandardCharsets.UTF_8);
        assertTrue("Ping-pong child timed out\n" + output, finished);
        assertEquals(output, 0, child.exitValue());
    }

    @Test
    public void zeroDurationStopsIdleReader() throws Exception {
        runChild(0, true);
    }

    @Test
    public void ordinaryRunStopsReaderBeforeDeletingFiles() throws Exception {
        runChild(1, true);
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
