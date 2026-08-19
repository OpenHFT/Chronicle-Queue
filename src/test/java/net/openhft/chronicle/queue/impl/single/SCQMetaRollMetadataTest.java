/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import org.junit.Test;

import java.time.LocalTime;
import java.time.ZoneId;

import static org.junit.Assert.assertEquals;

public class SCQMetaRollMetadataTest {

    @Test
    public void exposesPersistedRollMetadataWithoutExposingSCQRoll() {
        LocalTime rollTime = LocalTime.of(6, 30);
        ZoneId rollTimeZone = ZoneId.of("Europe/London");
        long epoch = 12_345L;
        SCQMeta metadata = new SCQMeta(new SCQRoll(
                TestRollCycles.TEST_DAILY, epoch, rollTime, rollTimeZone), 7);

        assertEquals(TestRollCycles.TEST_DAILY.lengthInMillis(),
                metadata.rollLengthInMillis());
        assertEquals(TestRollCycles.TEST_DAILY.format(), metadata.rollFormat());
        assertEquals(epoch, metadata.rollEpoch());
        assertEquals(rollTime, metadata.rollTime());
        assertEquals(rollTimeZone, metadata.rollTimeZone());
    }
}
