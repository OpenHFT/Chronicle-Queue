/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single.stress;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.*;

public class RollCycleMultiThreadStressPretouchTest extends RollCycleMultiThreadStressTest {

    @Override
    protected StressTestType stressTestType() {
        return StressTestType.PRETOUCH;
    }

    @Test
    public void stress() throws Exception {
        assumeTrue(SingleChronicleQueueBuilder.areEnterpriseFeaturesAvailable());
        super.stress();
        assertTrue(true); // parent has asserts
    }

    public static void main(String[] args) throws Exception {
        new RollCycleMultiThreadStressPretouchTest().run();
    }
}
