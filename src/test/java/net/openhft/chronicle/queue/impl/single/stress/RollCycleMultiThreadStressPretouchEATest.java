/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single.stress;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Assume;
import org.junit.Test;

public class RollCycleMultiThreadStressPretouchEATest extends RollCycleMultiThreadStressTest {

    public RollCycleMultiThreadStressPretouchEATest() {
        super(StressTestType.PRETOUCH_EA);
    }

    @Test
    public void stress() throws Exception {
        Assume.assumeTrue(SingleChronicleQueueBuilder.areEnterpriseFeaturesAvailable());
        super.stress();
    }

    public static void main(String[] args) throws Exception {
        new RollCycleMultiThreadStressPretouchEATest().run();
    }
}
