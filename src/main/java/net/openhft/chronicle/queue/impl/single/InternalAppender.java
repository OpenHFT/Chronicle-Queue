/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.queue.ExcerptAppender;

/**
 * This interface, {@code InternalAppender}, extends the {@link ExcerptAppender} and provides
 * additional functionality for appending entries at specific indices within a queue.
 * <p>
 * <strong>Note:</strong> This is an internal interface and should not be used externally
 * as it is subject to changes without notice.
 */
public interface InternalAppender extends ExcerptAppender {

    //! exactWritePreservesHistoricalSeal and exactWritePreservesIncompleteHeaderAfterQueueRestart exercise both
    //! boundaries. Replication traffic and retries cannot authorise overwriting the observed header.
    /**
     * Append at the exact next index in an open roll. A published duplicate is compared without changing the queue.
     * An append which encounters EOF or an incomplete record fails before changing either the roll or its indexes.
     * The same rules apply after restart. A missing highest or interior roll is rejected because its history is unknown.
     * <p>
     * If the index is:
     * <dl>
     *     <dt>Greater than the next valid index for the queue</dt>
     *     <dd>An {@link IllegalIndexException} is thrown</dd>
     *
     *     <dt>Less than or equal to the last index published before this call</dt>
     *     <dd>The supplied payload is compared with the authoritative published record and is
     *     never written. Equal content produces a debug message; different content produces a
     *     warning with both payloads as hex.</dd>
     *
     *     <dt>A ready record beyond the write-position publication boundary</dt>
     *     <dd>The requested ready record is compared using the same diagnostic policy and adopted
     *     without overwriting it if the roll has an open tail. Publication stops at that requested record.</dd>
     * </dl>
     *
     * @param index the exact queue index to append at
     * @param bytes the contents of the excerpt to write
     * @throws IllegalIndexException if the roll is open and {@code index} exceeds its next valid index
     * @throws IllegalArgumentException if the sequence is outside the roll cycle's declared capacity
     * @throws IllegalStateException if the target roll is absent within the retained published bounds
     * @throws ExactWriteBoundaryException if the write encounters EOF or an incomplete terminal header
     */
    void writeBytes(long index, BytesStore<?, ?> bytes);

}
