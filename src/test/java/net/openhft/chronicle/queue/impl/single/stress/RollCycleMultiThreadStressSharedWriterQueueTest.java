/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single.stress;

public class RollCycleMultiThreadStressSharedWriterQueueTest extends RollCycleMultiThreadStressTest {

    @Override
    protected StressTestType stressTestType() {
        return StressTestType.SHAREDWRITEQ;
    }

    public static void main(String[] args) throws Exception {
        new RollCycleMultiThreadStressSharedWriterQueueTest().run();
    }
}
