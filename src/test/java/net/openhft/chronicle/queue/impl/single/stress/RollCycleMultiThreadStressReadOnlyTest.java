/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single.stress;

import net.openhft.chronicle.core.OS;
import org.junit.Assume;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class RollCycleMultiThreadStressReadOnlyTest extends RollCycleMultiThreadStressTest {

    public RollCycleMultiThreadStressReadOnlyTest() {
        super(StressTestType.READONLY);
    }

    @Test
    public void stress() throws Exception {
        Assume.assumeFalse("Windows does not support read only", OS.isWindows());
        super.stress();
        assertTrue(true); // parent has asserts
    }

    public static void main(String[] args) throws Exception {
        new RollCycleMultiThreadStressReadOnlyTest().run();
    }
}
