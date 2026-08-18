/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

/**
 * Outcome of an attempt to park a named tailer.
 */
public enum NamedTailerParkResult {
    /** The named tailer existed and its persisted index was reset to zero. */
    PARKED,
    /** No persisted named tailer exists with the supplied name. */
    NOT_FOUND,
    /** The named tailer is replicated and cannot safely be parked locally. */
    REFUSED_REPLICATED,
    /** The supplied name is null or uses a reserved metadata suffix. */
    INVALID_NAME
}
