/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.metrics;

import net.openhft.chronicle.wire.SelfDescribingMarshallable;

/**
 * The {@code metricsFormat} header event written as the first excerpt of a dedicated metrics
 * queue: a magic string plus a format version, so a consumer (the metrics gateway, a tailer,
 * a test) can refuse an incompatible producer loudly instead of misreading its events.
 * <p>
 * The metric events themselves are self-describing and evolve additively within a
 * {@code formatVersion}; a non-additive change requires a version bump.
 */
public class MetricsFormat extends SelfDescribingMarshallable {

    /**
     * The expected magic string identifying a Chronicle metrics queue.
     */
    public static final String MAGIC = "chronicle-metrics";

    /**
     * The on-queue format version this producer writes.
     */
    public static final int FORMAT_VERSION = 1;

    private String magic = MAGIC;
    private int formatVersion = FORMAT_VERSION;

    /**
     * For Wire deserialization; constructs a header for the current format.
     */
    public MetricsFormat() {
    }

    /**
     * Returns the magic string carried by this header.
     *
     * @return the magic string
     */
    public String magic() {
        return magic;
    }

    /**
     * Sets the magic string; intended for Wire deserialization and tests only.
     *
     * @param magic the magic string
     * @return this instance for chaining
     */
    public MetricsFormat magic(String magic) {
        this.magic = magic;
        return this;
    }

    /**
     * Returns the format version carried by this header.
     *
     * @return the format version
     */
    public int formatVersion() {
        return formatVersion;
    }

    /**
     * Sets the format version; intended for Wire deserialization and tests only.
     *
     * @param formatVersion the format version
     * @return this instance for chaining
     */
    public MetricsFormat formatVersion(int formatVersion) {
        this.formatVersion = formatVersion;
        return this;
    }

    /**
     * Returns whether this header identifies a metrics queue this consumer can read:
     * the {@link #MAGIC} string and a version no newer than {@link #FORMAT_VERSION}.
     *
     * @return {@code true} if the header is compatible
     */
    public boolean compatible() {
        return MAGIC.equals(magic) && formatVersion >= 1 && formatVersion <= FORMAT_VERSION;
    }
}
