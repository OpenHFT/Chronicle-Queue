/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.QueueTestCommon;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ScanLimitRegressionTest extends QueueTestCommon {
    @Test
    public void positionScanWorksWithRestrictedWriteLimit() throws Exception {
        try (MaxPositionMutationTest.Fixture f = new MaxPositionMutationTest.Fixture(getTmpDir(), 13)) {
            Bytes<?> bytes = f.wireForIndex().bytes();
            long writeLimit = bytes.writeLimit();
            try {
                bytes.readLimit(f.positions[12] + 64);
                bytes.writeLimit(f.positions[12] + 64);
                assertEquals(12, f.lookup(Long.MAX_VALUE, true));
            } finally {
                bytes.writeLimit(writeLimit);
            }
        }
    }
}
