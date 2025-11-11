/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
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

    @Test
    public void formatAndParseEpochRoundTrip() {
        long epoch = 42_000L;
        String formatted = QueueOffsetSpec.formatEpochOffset(epoch);
        QueueOffsetSpec parsed = QueueOffsetSpec.parse(formatted);
        SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.single().queueOffsetSpec(parsed);
        assertEquals(epoch, builder.epoch());
        assertEquals(formatted, parsed.format());
    }

    @Test
    public void applyNoneDoesNotChangeBuilder() {
        QueueOffsetSpec spec = QueueOffsetSpec.ofNone();
        SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.single().queueOffsetSpec(spec);
        long originalEpoch = builder.epoch();
        assertEquals(originalEpoch, builder.epoch());
        assertEquals("NONE;", spec.format());
        assertEquals("NONE;", builder.queueOffsetSpec().format());
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseUnknownTypeThrows() {
        QueueOffsetSpec.parse("INVALID;foo");
    }

    @Test(expected = java.time.DateTimeException.class)
    public void parseRollTimeWithInvalidZoneFailsValidation() {
        QueueOffsetSpec spec = QueueOffsetSpec.parse("ROLL_TIME;12:00;Invalid/Zone");
        spec.validate();
    }
}
