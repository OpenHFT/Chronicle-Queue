/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

/**
 * This exception is thrown when a named tailer cannot be created due to specific reasons.
 * Named tailers are used in Chronicle Queue for named access patterns, and this exception
 * provides details on why the creation of such a tailer has failed.
 *
 * <p>The exception includes a {@link Reason} that describes why the tailer creation failed,
 * and provides the name of the tailer in question.
 */
public class NamedTailerNotAvailableException extends IllegalStateException {
    private static final long serialVersionUID = 0L;
    private final String tailerName;
    private final Reason reason;

    /**
     * Constructs a {@code NamedTailerNotAvailableException} with the given tailer name and reason.
     *
     * @param tailerName The name of the tailer that could not be created.
     * @param reason     The reason for the failure, as defined in {@link Reason}.
     */
    public NamedTailerNotAvailableException(String tailerName, Reason reason) {
        super("Named tailer cannot be created because: " + reason.description);
        this.tailerName = tailerName;
        this.reason = reason;
    }

    /**
     * Returns the name of the tailer that could not be created.
     *
     * @return The name of the unavailable tailer.
     */
    public String tailerName() {
        return tailerName;
    }

    /**
     * Returns the reason why the tailer could not be created.
     *
     * @return The {@link Reason} for the failure.
     */
    public Reason reason() {
        return reason;
    }

    /**
     * Enum representing the possible reasons why a named tailer cannot be created.
     */
    public enum Reason {

        /**
         * Indicates that named tailers cannot be created on a replication sink.
         */
        NOT_AVAILABLE_ON_SINK("Replicated named tailers cannot be instantiated on a replication sink");

        private final String description;

        /**
         * Constructs a {@code Reason} with the given description.
         *
         * @param description A brief description of the reason.
         */
        Reason(String description) {
            this.description = description;
        }
    }
}
