/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.queue.impl.table.Metadata;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

/**
 * SCQMeta class implements the Metadata interface and handles
 * metadata for SingleChronicleQueue, including roll settings,
 * delta checkpoint intervals, and source IDs. This class is
 * typically used to serialize and deserialize metadata.
 */
public class SCQMeta implements Metadata {

    // Represents the roll configuration for this metadata.
    @NotNull
    private final SCQRoll roll;

    // Source ID associated with the metadata.
    private int sourceId;

    /**
     * Constructor used via reflection. It reads the metadata fields
     * from the WireIn object and initializes the SCQMeta instance.
     *
     * @param wire WireIn object used to deserialize the metadata
     * @throws NullPointerException if roll is not provided in the wire input
     */
    @SuppressWarnings("unused")
    @UsedViaReflection
    SCQMeta(@NotNull WireIn wire) {
        this.roll = Objects.requireNonNull(wire.read(MetaDataField.roll).typedMarshallable());
        this.sourceId = wire.bytes().readRemaining() > 0 ? wire.read(MetaDataField.sourceId).int32() : 0;
    }

    /**
     * Constructs an SCQMeta object with the specified roll and source ID.
     *
     * @param roll the SCQRoll object for roll configuration
     * @param sourceId the source ID associated with this metadata
     */
    SCQMeta(@NotNull SCQRoll roll, int sourceId) {
        this.roll = roll;
        this.sourceId = sourceId;
    }

    /**
     * Returns the roll configuration associated with this metadata.
     *
     * @return the SCQRoll object
     */
    @NotNull
    public SCQRoll roll() {
        return roll;
    }

    /**
     * Returns the source ID associated with this metadata.
     *
     * @return the source ID
     */
    public int sourceId() {
        return sourceId;
    }

    /**
     * Writes the current state of the metadata to a WireOut object,
     * including roll, delta checkpoint interval, and source ID.
     *
     * @param wire the WireOut object to serialize the metadata to
     */
    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire
                .write(MetaDataField.roll).typedMarshallable(roll)
                .write(MetaDataField.sourceId).int32(this.sourceId);
    }

    /**
     * Overrides the metadata from the given SCQMeta object. Various
     * roll attributes and source ID are updated if different from
     * the current values. Warnings are logged if any values are overridden.
     *
     * @param metadata the SCQMeta object to override values from
     * @throws IllegalStateException if the provided metadata is not of type SCQMeta
     */
    @Override
    public <T extends Metadata> void overrideFrom(T metadata) {
        if (!(metadata instanceof SCQMeta))
            throw new IllegalStateException("Expected SCQMeta, got " + metadata.getClass());

        SCQMeta other = (SCQMeta) metadata;

        // Override roll epoch if different
        SCQRoll roll = other.roll;
        if (roll.epoch() != this.roll.epoch()) {
            Jvm.warn().on(getClass(), "Overriding roll epoch from existing metadata, was " + this.roll.epoch() + ", overriding to " + roll.epoch());
            this.roll.epoch(roll.epoch());
        }

        // Override roll length if different
        if (roll.length() != this.roll.length()) {
            Jvm.warn().on(getClass(), "Overriding roll length from existing metadata, was " + this.roll.length() + ", overriding to " + roll.length());
            this.roll.length(roll.length());
            this.roll.format(roll.format());
        }

        // Override roll time if different
        if (roll.rollTime() != null && !Objects.equals(roll.rollTime(), this.roll.rollTime())) {
            Jvm.warn().on(getClass(), "Overriding roll time from existing metadata, was " + this.roll.rollTime() + ", overriding to " + roll.rollTime());
            this.roll.rollTime(roll.rollTime());
        }

        // Override roll time zone if different
        if (roll.rollTimeZone() != null && !Objects.equals(roll.rollTimeZone(), this.roll.rollTimeZone())) {
            Jvm.warn().on(getClass(), "Overriding roll time zone from existing metadata, was " + this.roll.rollTimeZone() + ", overriding to " + roll.rollTimeZone());
            this.roll.rollTimeZone(roll.rollTimeZone());
        }

        // Override sourceId if different
        if (other.sourceId != sourceId) {
            Jvm.warn().on(getClass(), "Overriding sourceId from existing metadata, was " + sourceId + ", overriding to " + other.sourceId);
            this.sourceId = other.sourceId;
        }
    }
}
