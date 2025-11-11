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
     * <p>
     * If the index is:
     * <dl>
     *     <dt>Greater than the next valid indices for the queue</dt>
     *     <dd>An {@link IllegalIndexException} is thrown</dd>
     *
     *     <dt>Less than or equal to the last index in the queue</dt>
     *     <dd>The method returns without modifying the queue</dd>
     * </dl>
     *
     * @param index index the index to append at
     * @param bytes bytes the contents of the excerpt to write
     * @throws IllegalIndexException if the index specified is larger than the valid next indices of the queue
     */
    void writeBytes(long index, BytesStore<?, ?> bytes);

}
