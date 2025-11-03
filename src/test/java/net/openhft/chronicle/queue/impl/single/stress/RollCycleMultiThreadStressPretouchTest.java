/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single.stress;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Assume;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class RollCycleMultiThreadStressPretouchTest extends RollCycleMultiThreadStressTest {

    public RollCycleMultiThreadStressPretouchTest() {
        super(StressTestType.PRETOUCH);
    }

    @Test
    public void stress() throws Exception {
        Assume.assumeTrue(SingleChronicleQueueBuilder.areEnterpriseFeaturesAvailable());
        super.stress();
        assertTrue(true); // parent has asserts
    }

    public static void main(String[] args) throws Exception {
        new RollCycleMultiThreadStressPretouchTest().run();
    }
}
