/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import static org.junit.Assert.*;

import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.rollcycles.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Files;

public class RollCycleFileInferenceTest {

    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private RollCycle infer() throws Exception {
        SingleChronicleQueueBuilder builder =
                SingleChronicleQueueBuilder.binary(temporary.getRoot())
                        .rollCycle(TestRollCycles.TEST_SECONDLY);
        Method method =
                SingleChronicleQueueBuilder.class.getDeclaredMethod(
                        "validateRollCycle", File.class);
        method.setAccessible(true);
        method.invoke(builder, new File(temporary.getRoot(), "metadata.cq4t"));
        return builder.rollCycle();
    }

    @Test
    public void rollInferenceIgnoresDirectories() throws Exception {
        Files.createDirectory(new File(temporary.getRoot(), "20260101F.cq4").toPath());
        assertSame(TestRollCycles.TEST_SECONDLY, infer());
    }

    @Test
    public void rollInferenceRecognisesExistingRollFile() throws Exception {
        Files.createFile(new File(temporary.getRoot(), "20260101F.cq4").toPath());
        assertSame(RollCycles.FAST_DAILY, infer());
    }

    @Test
    public void legacyRollCycleRemainsResolvable() throws Exception {
        assertSame(LegacyRollCycles.DAILY, RollCycles.valueOfEnum("DAILY"));
    }
}
