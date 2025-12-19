/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.domestic;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.jupiter.api.Test;

import java.time.LocalTime;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings({"deprecation", "removal"})
public class QueueOffsetSpecTest {

    @Test
    public void parseEpochAndApplySetsBuilderEpoch() {
        QueueOffsetSpec spec = QueueOffsetSpec.parse("EPOCH;12345");
        SingleChronicleQueueBuilder b = SingleChronicleQueueBuilder.single();
        spec.apply(b);
        assertEquals(12345L, b.epoch(), "parsed epoch offset should be applied to builder configuration");
        spec.validate();
        assertEquals("EPOCH;12345", QueueOffsetSpec.formatEpochOffset(12345L), "epoch offset should be formatted with EPOCH prefix and semicolon separator");
    }

    @Test
    public void parseRollTimeAndApplySetsRollTimeAndZone() {
        String def = "ROLL_TIME;12:34;Europe/London";
        QueueOffsetSpec spec = QueueOffsetSpec.parse(def);
        SingleChronicleQueueBuilder b = SingleChronicleQueueBuilder.single();
        spec.apply(b);
        // epoch becomes seconds-of-day offset
        long expectedMs = LocalTime.parse("12:34").toSecondOfDay() * 1000L;
        assertEquals(expectedMs, b.epoch(), "roll time should be converted to milliseconds-since-midnight for epoch offset");
        assertEquals(ZoneId.of("Europe/London"), b.rollTimeZone(), "parsed time zone should be applied to builder roll time zone");
        spec.validate();
        assertEquals("ROLL_TIME;12:34;Europe/London",
                QueueOffsetSpec.formatRollTime(LocalTime.parse("12:34"), ZoneId.of("Europe/London")),
                "roll time should be formatted with ROLL_TIME prefix, time, and zone separated by semicolons");
    }

    @Test
    public void formatNoneReturnsBareType() {
        assertEquals("NONE", QueueOffsetSpec.formatNone(), "none type should be formatted as bare keyword without parameters");
    }

    public void parseWithTooFewTokensThrows() {
        assertThrows(IllegalArgumentException.class, () -> QueueOffsetSpec.parse("ROLL_TIME;12:00"), "parsing should fail when roll time specification is missing required time zone parameter");
    }

    @Test
    public void formatAndParseEpochRoundTrip() {
        long epoch = 42_000L;
        String formatted = QueueOffsetSpec.formatEpochOffset(epoch);
        QueueOffsetSpec parsed = QueueOffsetSpec.parse(formatted);
        SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.single().queueOffsetSpec(parsed);
        assertEquals(epoch, builder.epoch(), "epoch value should be preserved when formatted and parsed back");
        assertEquals(formatted, parsed.format(), "formatted representation should remain identical after parse round-trip");
    }

    @Test
    public void applyNoneDoesNotChangeBuilder() {
        QueueOffsetSpec spec = QueueOffsetSpec.ofNone();
        SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.single().queueOffsetSpec(spec);
        long originalEpoch = builder.epoch();
        assertEquals(originalEpoch, builder.epoch(), "none offset spec should leave builder epoch unchanged");
        assertEquals("NONE;", spec.format(), "none spec should format with trailing semicolon");
        assertEquals("NONE;", builder.queueOffsetSpec().format(), "builder should preserve none spec format with trailing semicolon");
    }

    public void parseUnknownTypeThrows() {
        assertThrows(IllegalArgumentException.class, () -> QueueOffsetSpec.parse("INVALID;foo"), "parsing should reject unrecognized offset spec type identifiers");
    }

    public void parseRollTimeWithInvalidZoneFailsValidation() {
        QueueOffsetSpec spec = QueueOffsetSpec.parse("ROLL_TIME;12:00;Invalid/Zone");
        assertThrows(java.time.DateTimeException.class, spec::validate, "validation should fail when roll time spec contains malformed time zone identifier");
    }
}
