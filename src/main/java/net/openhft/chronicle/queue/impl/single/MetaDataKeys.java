/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.wire.WireKey;

/**
 * The {@code MetaDataKeys} enum defines keys that are used to represent different sections
 * or types of metadata within a Chronicle Queue. These keys help in identifying and accessing
 * specific metadata components such as headers, indexes, and roll information.
 */
public enum MetaDataKeys implements WireKey {

    /**
     * Represents the key for the queue's header metadata. The header typically contains
     * information about the queue's structure and configuration.
     */
    header,

    /**
     * Represents the key for the index-to-index structure within the queue, which manages
     * the link between different index entries and assists in efficient navigation.
     */
    index2index,

    /**
     * Represents the key for the index, which keeps track of the positions of various
     * excerpts in the queue for quick lookups and reads.
     */
    index,

    /**
     * Represents the key for the roll metadata, which manages the queue's rolling behavior,
     * determining when the queue rolls over to the next cycle.
     */
    roll
}
