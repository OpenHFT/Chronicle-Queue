/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.main;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import net.openhft.chronicle.queue.main.RemovableRollFileCandidatesMain;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class InternalRemovableRollFileCandidatesMainTest extends QueueTestCommon {

    @Test
    @DisplayName("Internal main prints removable roll files")
    public void internalMainPrintsRemovableFiles() {
        assumeTrue(OS.isLinux() && !isWsl(), "Test relies on Linux file handling for internal main output");

        final File dir = prepareQueueWithMultipleCycles();

        final String output = invokeMain(InternalRemovableRollFileCandidatesMain::main, dir.getAbsolutePath());

        assertFalse(output.trim().isEmpty(), "Removable candidates output should not be empty");
        assertTrue(output.contains(dir.getAbsolutePath()),
                "Output should include queue path " + dir.getAbsolutePath() + ": " + output);
    }

    @Test
    @DisplayName("Public main delegates to internal main")
    public void publicMainDelegatesToInternal() {
        assumeTrue(OS.isLinux() && !isWsl(), "Test relies on Linux file handling for public main delegation");

        final File dir = prepareQueueWithMultipleCycles();

        final String internalOutput = invokeMain(InternalRemovableRollFileCandidatesMain::main, dir.getAbsolutePath());
        final String publicOutput = invokeMain(RemovableRollFileCandidatesMain::main, dir.getAbsolutePath());

        assertEquals(internalOutput, publicOutput, "public main should delegate to internal");
    }

    private File prepareQueueWithMultipleCycles() {
        final File dir = getTmpDir();
        final AtomicLong time = new AtomicLong(System.currentTimeMillis());
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(dir)
                .rollCycle(TestRollCycles.TEST_SECONDLY)
                .timeProvider(time::get)
                .build();
             ExcerptAppender appender = queue.createAppender()) {
            appender.writeText("first");
            time.addAndGet(1_000);
            appender.writeText("second");
        }
        return dir;
    }

    private String invokeMain(Consumer<String[]> main, String... args) {
        final PrintStream originalOut = System.out;
        final ByteArrayOutputStream capture = new ByteArrayOutputStream();
        System.setOut(new PrintStream(capture));
        try {
            main.accept(args);
        } finally {
            System.setOut(originalOut);
        }
        return capture.toString();
    }
}
