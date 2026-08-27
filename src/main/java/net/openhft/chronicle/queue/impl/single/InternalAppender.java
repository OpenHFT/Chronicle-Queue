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
     * missing index. Replacing the marker is logged as a warning; the cycle remains open for the
     * rest of the backfill and is resealed by {@link #normaliseEOFs()} at completion.
     * <p>
     * For queues that support this path, {@code StoreAppender} establishes padding when it
     * constructs the Wire. Exact-index recovery has no unpadded mode.
     * <p>
     * Queue serialises concurrent backfill appenders with its write lock. A published index is a
     * first-writer-wins retry: matching content returns silently and different content emits a
     * warning without overwriting the existing entry. A gap is rejected.
     * If an attempt leaves the requested header incomplete, a later exact-index call can replace it,
     * including from a new Queue instance after restart. That retry does not infer whether the failed
     * attempt opened an end-of-data marker, so restoring any missing marker is a separate completion step.
     * The caller must retry after failure, call {@link #normaliseEOFs()} before publishing recovery
     * completion, and exclude archive/delete maintenance throughout that interval. EOF is a hard
     * seal for ordinary writes, not proof that exact recovery can never reopen the roll.
     * <p>
     * If the index is:
     * <dl>
     *     <dt>Greater than the next valid index for the queue</dt>
     *     <dd>An {@link IllegalIndexException} is thrown</dd>
     *
     *     <dt>Less than or equal to the last index published before this call</dt>
     *     <dd>The existing entry is compared. Matching content returns silently; different content
     *     emits a warning and remains first-writer-wins. A ready first-writer record left beyond the
     *     write-position publication boundary is adopted successfully without overwriting it.</dd>
     * </dl>
     *
     * @param index the exact queue index to append at
     * @param bytes the contents of the excerpt to write
     * @throws IllegalIndexException if {@code index} is greater than the next valid queue index
     * @throws IllegalArgumentException if the sequence is outside the roll cycle's declared capacity
     */
    void writeBytes(long index, BytesStore<?, ?> bytes);

}
