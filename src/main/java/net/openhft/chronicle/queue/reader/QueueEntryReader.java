/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.reader;

/**
 * Interface for reading and processing entries from a queue.
 * <p>Implementations of this interface are responsible for reading the next available entry
 * from the queue and processing it as necessary.
 */
public interface QueueEntryReader {

    /**
     * Reads and processes the next entry from the queue.
     *
     * @return {@code true} if there was an entry to read and process, {@code false} if the end of the queue has been reached
     */
    boolean read();
}
