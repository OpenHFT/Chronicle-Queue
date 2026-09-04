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

    //! exactWriteReplacesAnIncompleteRequestedEntryAfterQueueRestart, canBackfillPreviousCycleAfterEOF and
    //! historicalWriteAfterReadyInterruptedRecordNormalisesAtCompletion require the internal API contract to expose
    //! retry, physical recovery and completion responsibilities. Without them a replication caller could treat EOF
    //! as immutable or acknowledge a backfill before resealing it; the implementation decisions are justified beside
    //! their respective branches rather than by this interface-level documentation note.
    //! InternalAppenderWriteBytesTest#publishedDuplicatePayloadsAreComparedWithoutMutation requires this contract to
    //! state that duplicate comparison is diagnostic and never rejects or overwrites an already-published record.
    //! exactWriteDoesNotRecreateDeletedPublishedMaximum and exactWriteRejectsAbsentCycleWithinPublishedRange require
    //! the same contract to distinguish a creatable new sparse roll from missing retained publication state.
    /**
     * Append an excerpt at the specified index, if the index is a valid next index for the queue.
     * This internal replication path can replace an end-of-data marker when restoring an exact
     * missing index. Replacing the marker is logged as a warning; the cycle remains open for the
     * rest of the backfill and is resealed by {@link #normaliseEOFs()} at completion.
     * <p>
     * For queues that support this path, {@code StoreAppender} establishes padding when it
     * constructs the Wire. Exact-index recovery has no unpadded mode.
     * <p>
     * Queue serialises concurrent backfill appenders with its write lock. A published index is
     * authoritative and treated as already applied; a gap is rejected. A duplicate is compared
     * with the published payload: equal content is reported at debug level, while different
     * content is warned with both payloads as hex and the supplied duplicate is ignored.
     * An absent roll may be created only outside the retained published bounds. An absent
     * highest roll or interior roll within those bounds is rejected because Queue cannot
     * distinguish unsupported deletion from a never-created sparse generation.
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
     *     <dd>The supplied payload is compared with the authoritative published record and is
     *     never written. Equal content produces a debug message; different content produces a
     *     warning with both payloads as hex. A distinct ready first-writer record left beyond the
     *     write-position publication boundary is adopted successfully without overwriting it.</dd>
     * </dl>
     *
     * @param index the exact queue index to append at
     * @param bytes the contents of the excerpt to write
     * @throws IllegalIndexException if {@code index} is greater than the next valid queue index
     * @throws IllegalArgumentException if the sequence is outside the roll cycle's declared capacity
     * @throws IllegalStateException if the target roll is absent within the retained published bounds
     */
    void writeBytes(long index, BytesStore<?, ?> bytes);

}
