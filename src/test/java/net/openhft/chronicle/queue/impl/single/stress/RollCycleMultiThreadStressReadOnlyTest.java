/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single.stress;

import net.openhft.chronicle.core.OS;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class RollCycleMultiThreadStressReadOnlyTest extends RollCycleMultiThreadStressTest {

    public RollCycleMultiThreadStressReadOnlyTest() {
        super(StressTestType.READONLY);
    }

    @Test
    @Override
    public void stress() throws Exception {
        Assumptions.assumeFalse(OS.isWindows(), "Windows does not support read only");
        super.stress();
        assertTrue(true, "stress: assertions are in parent"); // parent has asserts
    }

    public static void main(String[] args) throws Exception {
        new RollCycleMultiThreadStressReadOnlyTest().run();
    }
}
