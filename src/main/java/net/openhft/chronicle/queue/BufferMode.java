/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

/**
 * Enum representing the different buffer modes that can be used within Chronicle Queue.
 * Each mode has a specific use case and behavior depending on the configuration.
 */
public enum BufferMode {
    /**
     * The default buffer mode.
     * No additional buffering or special handling is applied.
     */
    None,

    /**
     * Buffer mode used in conjunction with encryption.
     * Data is copied into a buffer before being processed.
     */
    Copy,

    /**
     * Buffer mode used for asynchronous processing.
     * This mode is specific to Chronicle Ring, an enterprise product.
     */
    Asynchronous
}
