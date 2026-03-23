/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single.stress;

import net.openhft.chronicle.core.OS;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.*;

public class RollCycleMultiThreadStressReadOnlyTest extends RollCycleMultiThreadStressTest {

    @Override
    protected StressTestType stressTestType() {
        return StressTestType.READONLY;
    }

    @Test
    public void stress() throws Exception {
        assumeFalse(OS.isWindows(), "Windows does not support read only");
        super.stress();
        assertTrue(true); // parent has asserts
    }

    public static void main(String[] args) throws Exception {
        new RollCycleMultiThreadStressReadOnlyTest().run();
    }
}
