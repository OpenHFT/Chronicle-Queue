/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.LocalTime;
import java.time.ZoneId;

/**
 * SCQRoll class handles roll settings for the SingleChronicleQueue,
 * such as the roll length, format, epoch time, roll time, and time zone.
 * It implements {@link Demarshallable} and {@link WriteMarshallable}
 * to allow reading and writing these fields to a wire format.
 */
class SCQRoll implements Demarshallable, WriteMarshallable {

    // The length of the roll in milliseconds.
    private int length;

    // The format used for roll naming, nullable.
    @Nullable
    private String format;

    // The time at which the roll occurs, nullable.
    @Nullable
    private LocalTime rollTime;

    // The time zone for the roll time, nullable.
    @Nullable
    private ZoneId rollTimeZone;

    // The epoch offset in milliseconds since January 1, 1970.
    private long epoch;

    /**
     * Constructor used via reflection, typically for deserialization.
     * Reads the roll settings from a {@link WireIn} object.
     *
     * @param wire the WireIn object used to read roll fields
     */
    @UsedViaReflection
    private SCQRoll(@NotNull WireIn wire) {
        length = wire.read(RollFields.length).int32();
        format = wire.read(RollFields.format).text();
        epoch = wire.read(RollFields.epoch).int64();

        // Read rollTime if it exists in the wire
        ValueIn rollTimeVIN = wire.read(RollFields.rollTime);
        if (rollTimeVIN.hasNext())
            rollTime = rollTimeVIN.time();

        // Read rollTimeZone if available
        String zoneId = wire.read(RollFields.rollTimeZone).text();
        if (zoneId != null)
            rollTimeZone = ZoneId.of(zoneId);
        else
            rollTimeZone = null;
    }

    /**
     * Constructs an SCQRoll with the provided roll cycle, epoch, roll time, and roll time zone.
     *
     * @param rollCycle the RollCycle object to derive the roll length and format
     * @param epoch the epoch offset in milliseconds
     * @param rollTime the time of the roll, nullable
     * @param rollTimeZone the time zone for the roll, nullable
     */
    SCQRoll(@NotNull RollCycle rollCycle,
            long epoch,
            @Nullable LocalTime rollTime,
            @Nullable ZoneId rollTimeZone) {
        this.length = rollCycle.lengthInMillis();
        this.format = rollCycle.format();
        this.epoch = epoch;
        this.rollTime = rollTime;
        this.rollTimeZone = rollTimeZone;
    }

    /**
     * Writes the current state of the SCQRoll to a {@link WireOut} object, including
     * length, format, epoch, roll time, and roll time zone.
     *
     * @param wire the WireOut object to write the roll fields to
     */
    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write(RollFields.length).int32(length)
                .write(RollFields.format).text(format)
                .write(RollFields.epoch).int64(epoch);

        // Write rollTime if not null
        if (rollTime != null)
            wire.write(RollFields.rollTime).time(rollTime);

        // Write rollTimeZone if not null
        if (rollTimeZone != null)
            wire.write(RollFields.rollTimeZone).text(rollTimeZone.getId());
    }

    /**
     * Returns the epoch offset as the number of milliseconds since January 1, 1970.
     *
     * @return the epoch time in milliseconds
     */
    public long epoch() {
        return this.epoch;
    }

    /**
     * Returns the format used for roll naming.
     *
     * @return the format as a string, or null if not set
     */
    public String format() {
        return this.format;
    }

    /**
     * Returns the roll length in milliseconds.
     *
     * @return the length of the roll
     */
    int length() {
        return length;
    }

    /**
     * Returns the time at which the roll occurs, or null if not set.
     *
     * @return the roll time, nullable
     */
    @Nullable
    public LocalTime rollTime() {
        return rollTime;
    }

    /**
     * Returns the time zone for the roll time, or null if not set.
     *
     * @return the roll time zone, nullable
     */
    @Nullable
    public ZoneId rollTimeZone() {
        return rollTimeZone;
    }

    /**
     * Sets the length of the roll in milliseconds.
     *
     * @param length the new length to set
     */
    public void length(int length) {
        this.length = length;
    }

    /**
     * Sets the format for the roll naming.
     *
     * @param format the new format to set, nullable
     */
    public void format(@Nullable String format) {
        this.format = format;
    }

    /**
     * Sets the roll time.
     *
     * @param rollTime the new roll time to set, nullable
     */
    public void rollTime(@Nullable LocalTime rollTime) {
        this.rollTime = rollTime;
    }

    /**
     * Sets the roll time zone.
     *
     * @param rollTimeZone the new time zone to set, nullable
     */
    public void rollTimeZone(@Nullable ZoneId rollTimeZone) {
        this.rollTimeZone = rollTimeZone;
    }

    /**
     * Sets the epoch time.
     *
     * @param epoch the new epoch to set in milliseconds
     */
    public void epoch(long epoch) {
        this.epoch = epoch;
    }

    /**
     * Returns a string representation of the SCQRoll object, displaying all roll fields.
     *
     * @return a string representation of the object
     */
    @Override
    public String toString() {
        return "SCQRoll{" +
                "length=" + length +
                ", format='" + format + '\'' +
                ", epoch=" + epoch +
                ", rollTime=" + rollTime +
                ", rollTimeZone=" + rollTimeZone +
                '}';
    }

    /**
     * Enum representing the fields used for marshalling and demarshalling roll data.
     */
    enum RollFields implements WireKey {
        length, format, epoch, rollTime, rollTimeZone
    }
}
