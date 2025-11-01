/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.domestic;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Test;

import java.time.LocalTime;
import java.time.ZoneId;

import static org.junit.Assert.*;

public class QueueOffsetSpecTest {

    @Test
    public void parseEpochAndApplySetsBuilderEpoch() {
        QueueOffsetSpec spec = QueueOffsetSpec.parse("EPOCH;12345");
        SingleChronicleQueueBuilder b = SingleChronicleQueueBuilder.single();
        spec.apply(b);
        assertEquals(12345L, b.epoch());
        spec.validate();
        assertEquals("EPOCH;12345", QueueOffsetSpec.formatEpochOffset(12345L));
    }

    @Test
    public void parseRollTimeAndApplySetsRollTimeAndZone() {
        String def = "ROLL_TIME;12:34;Europe/London";
        QueueOffsetSpec spec = QueueOffsetSpec.parse(def);
        SingleChronicleQueueBuilder b = SingleChronicleQueueBuilder.single();
        spec.apply(b);
        // epoch becomes seconds-of-day offset
        long expectedMs = LocalTime.parse("12:34").toSecondOfDay() * 1000L;
        assertEquals(expectedMs, b.epoch());
        assertEquals(ZoneId.of("Europe/London"), b.rollTimeZone());
        spec.validate();
        assertEquals("ROLL_TIME;12:34;Europe/London", QueueOffsetSpec.formatRollTime(LocalTime.parse("12:34"), ZoneId.of("Europe/London")));
    }

    @Test
    public void formatNoneReturnsBareType() {
        assertEquals("NONE", QueueOffsetSpec.formatNone());
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseWithTooFewTokensThrows() {
        QueueOffsetSpec.parse("ROLL_TIME;12:00");
    }
}

