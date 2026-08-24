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

    /**
     * Append an excerpt at the specified index, if the index is a valid next index for the queue.
     * This internal replication path can replace an end-of-data marker when restoring an exact
     * missing index. Replacing the marker is logged as a warning and the cycle is resealed only after
     * the data record and its sparse-index metadata are committed.
     * <p>
     * Queue serialises concurrent backfill appenders with its write lock. The first committed value
     * at an index wins; later duplicates return without comparing the supplied bytes.
     * <p>
     * If the index is:
     * <dl>
     *     <dt>Greater than the next valid indices for the queue</dt>
     *     <dd>An {@link IllegalIndexException} is thrown</dd>
     *
     *     <dt>Less than or equal to the last index in the queue</dt>
     *     <dd>The method returns without modifying the queue. The first committed record remains
     *     authoritative and the supplied duplicate bytes are not compared.</dd>
     * </dl>
     *
     * @param index index the index to append at
     * @param bytes bytes the contents of the excerpt to write
     * @throws IllegalIndexException if the index specified is larger than the valid next indices of the queue
     */
    void writeBytes(long index, BytesStore<?, ?> bytes);

}
