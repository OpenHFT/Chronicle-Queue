/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single.stress;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class RollCycleMultiThreadStressPretouchEATest extends RollCycleMultiThreadStressTest {

    public RollCycleMultiThreadStressPretouchEATest() {
        super(StressTestType.PRETOUCH_EA);
    }

    @Test
    @DisplayName("Pretouch EA stress test runs under enterprise features")
    @Override
    public void stress() throws Exception {
        Assumptions.assumeTrue(SingleChronicleQueueBuilder.areEnterpriseFeaturesAvailable(),
                "Enterprise features are required for PRETOUCH_EA");
        super.stress();
        assertTrue(true, "stress: assertions are in parent"); // parent has asserts
    }

    public static void main(String[] args) throws Exception {
        new RollCycleMultiThreadStressPretouchEATest().run();
    }
}
