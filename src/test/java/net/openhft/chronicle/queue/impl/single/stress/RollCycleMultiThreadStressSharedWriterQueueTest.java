/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single.stress;

import org.junit.Test;

public class RollCycleMultiThreadStressSharedWriterQueueTest extends RollCycleMultiThreadStressTest {

    public RollCycleMultiThreadStressSharedWriterQueueTest() {
        super(StressTestType.SHAREDWRITEQ);
    }

    public static void main(String[] args) throws Exception {
        new RollCycleMultiThreadStressSharedWriterQueueTest().run();
    }
}
