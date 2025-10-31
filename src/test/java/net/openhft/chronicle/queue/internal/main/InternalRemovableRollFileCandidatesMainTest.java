/*
 * Copyright 2016-2025 chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue.internal.main;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import net.openhft.chronicle.queue.main.RemovableRollFileCandidatesMain;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

public class InternalRemovableRollFileCandidatesMainTest extends QueueTestCommon {

    @Test
    public void internalMainPrintsRemovableFiles() {
        assumeTrue(OS.isLinux());

        final File dir = prepareQueueWithMultipleCycles();

        final String output = invokeMain(InternalRemovableRollFileCandidatesMain::main, dir.getAbsolutePath());

        assertFalse("Expected removable candidates to be printed", output.trim().isEmpty());
        assertTrue(output.contains(dir.getAbsolutePath()));
    }

    @Test
    public void publicMainDelegatesToInternal() {
        assumeTrue(OS.isLinux());

        final File dir = prepareQueueWithMultipleCycles();

        final String internalOutput = invokeMain(InternalRemovableRollFileCandidatesMain::main, dir.getAbsolutePath());
        final String publicOutput = invokeMain(RemovableRollFileCandidatesMain::main, dir.getAbsolutePath());

        assertEquals(internalOutput, publicOutput);
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
