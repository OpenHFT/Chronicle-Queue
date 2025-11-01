/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.domestic;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.jetbrains.annotations.NotNull;

import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.function.Function;

/**
 * The {@code QueueOffsetSpec} class represents a specification for defining the offset of a Chronicle Queue.
 * The offset can be defined based on an epoch, roll time, or none.
 * It provides methods to create, parse, and apply different types of offsets to a {@link SingleChronicleQueueBuilder}.
 *
 * <p> The class is final, and instances are immutable.
 */
public final class QueueOffsetSpec {

    private static final String TOKEN_DELIMITER = ";";
    private final Type type;
    private final String[] spec;

    /**
     * Private constructor for initializing {@code QueueOffsetSpec} with a type and corresponding spec arguments.
     *
     * @param type the type of offset (EPOCH, ROLL_TIME, or NONE)
     * @param spec the corresponding arguments for the offset type
     */
    private QueueOffsetSpec(final Type type, final String[] spec) {
        this.type = type;
        this.spec = spec;
    }

    /**
     * Creates a {@code QueueOffsetSpec} based on a given epoch time.
     *
     * @param epoch the epoch time in milliseconds
     * @return a new {@code QueueOffsetSpec} instance for epoch-based offset
     */
    public static QueueOffsetSpec ofEpoch(final long epoch) {
        return new QueueOffsetSpec(Type.EPOCH, new String[]{Long.toString(epoch)});
    }

    /**
     * Creates a {@code QueueOffsetSpec} based on a roll time and zone ID.
     *
     * @param time   the time of the day to roll
     * @param zoneId the zone ID for the time zone
     * @return a new {@code QueueOffsetSpec} instance for roll-time-based offset
     */
    public static QueueOffsetSpec ofRollTime(@NotNull final LocalTime time, @NotNull final ZoneId zoneId) {
        return new QueueOffsetSpec(Type.ROLL_TIME, new String[]{time.toString(), zoneId.toString()});
    }

    /**
     * Creates a {@code QueueOffsetSpec} representing no offset.
     *
     * @return a new {@code QueueOffsetSpec} instance representing no offset
     */
    public static QueueOffsetSpec ofNone() {
        return new QueueOffsetSpec(Type.NONE, new String[]{});
    }

    /**
     * Parses a string definition to create a corresponding {@code QueueOffsetSpec}.
     *
     * @param definition the string representation of the offset specification
     * @return a new {@code QueueOffsetSpec} instance parsed from the string
     * @throws IllegalArgumentException if the string format is invalid or the type is unknown
     */
    public static QueueOffsetSpec parse(@NotNull final String definition) {
        final String[] tokens = definition.split(TOKEN_DELIMITER);
        final Type type = Type.valueOf(tokens[0]);
        switch (type) {
            case EPOCH:
                expectArgs(tokens, 2);
                return new QueueOffsetSpec(type, new String[]{tokens[1]});
            case ROLL_TIME:
                expectArgs(tokens, 3);
                return new QueueOffsetSpec(type, new String[]{tokens[1], tokens[2]});
            case NONE:
                expectArgs(tokens, 1);
                return new QueueOffsetSpec(type, new String[]{});
            default:
                throw new IllegalArgumentException("Unknown type: " + type);
        }
    }

    /**
     * Formats an epoch-based offset as a string.
     *
     * @param epochOffset the epoch offset value
     * @return the formatted string representing the epoch offset
     */
    public static String formatEpochOffset(final long epochOffset) {
        return String.format("%s;%s", Type.EPOCH.name(), epochOffset);
    }

    /**
     * Formats a roll-time-based offset as a string.
     *
     * @param time   the time of day for roll
     * @param zoneId the time zone ID
     * @return the formatted string representing the roll time
     */
    public static String formatRollTime(final LocalTime time, final ZoneId zoneId) {
        return String.format("%s;%s;%s", Type.ROLL_TIME.name(), time.toString(), zoneId.toString());
    }

    /**
     * Formats a "none" offset type as a string.
     *
     * @return the formatted string representing the "none" offset
     */
    public static String formatNone() {
        return Type.NONE.name();
    }

    private static ZoneId toZoneId(final String zoneId) {
        return ZoneId.of(zoneId);
    }

    private static LocalTime toLocalTime(final String timestamp) {
        return LocalTime.parse(timestamp);
    }

    private static void expectArgs(final String[] tokens, final int expectedLength) {
        if (tokens.length != expectedLength) {
            throw new IllegalArgumentException("Expected " + expectedLength + " tokens in " + Arrays.toString(tokens));
        }
    }

    /**
     * Applies the current offset specification to a {@link SingleChronicleQueueBuilder}.
     *
     * @param builder the queue builder to apply the offset to
     * @throws IllegalArgumentException if the offset type is unknown
     */
    public void apply(final SingleChronicleQueueBuilder builder) {
        switch (type) {
            case EPOCH:
                builder.epoch(Long.parseLong(spec[0]));
                break;
            case ROLL_TIME:
                builder.rollTime(toLocalTime(spec[0]), toZoneId(spec[1]));
                break;
            case NONE:
                break;
            default:
                throw new IllegalArgumentException("Unknown type: " + type);
        }
    }

    /**
     * Formats the current offset specification as a string.
     *
     * @return the formatted string representing the offset specification
     */
    public String format() {
        return type.name() + TOKEN_DELIMITER + type.argFormatter.apply(spec);
    }

    /**
     * Validates the current offset specification by checking the correctness of its arguments.
     *
     * @throws IllegalArgumentException if the offset arguments are invalid
     */
    public void validate() {
        switch (type) {
            case EPOCH:
                Long.parseLong(spec[0]);
                break;
            case ROLL_TIME:
                toLocalTime(spec[0]);
                toZoneId(spec[1]);
                break;
            case NONE:
                break;
            default:
                throw new IllegalArgumentException("Unknown type: " + type);
        }
    }

    /**
     * The {@code Type} enum represents the possible types of queue offset specifications:
     * - EPOCH: Based on an epoch timestamp
     * - ROLL_TIME: Based on a roll time and zone ID
     * - NONE: No offset
     */
    public enum Type {
        EPOCH(args -> args[0]),
        ROLL_TIME(args -> args[0] + TOKEN_DELIMITER + args[1]),
        NONE(args -> "");

        private final Function<String[], String> argFormatter;

        Type(final Function<String[], String> argFormatter) {
            this.argFormatter = argFormatter;
        }
    }
}
