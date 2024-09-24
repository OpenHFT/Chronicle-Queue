/*
 * Copyright 2016-2020 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue;

import net.openhft.chronicle.wire.*;

/**
 * A singleton implementation of the {@link MessageHistory} interface that represents a "no-op" or "empty" message history.
 * This is used when message history tracking is not required. All methods in this class return default values,
 * indicating no history is being stored.
 * <p>
 * It suppresses warnings for deprecated methods, though this may need to be verified in the broader context to ensure
 * it's still appropriate.
 */
@SuppressWarnings("deprecation") // Suppressing warnings related to deprecated methods
public enum NoMessageHistory implements MessageHistory {
    /**
     * The single instance of {@code NoMessageHistory}, following the singleton pattern.
     */
    INSTANCE;

    /**
     * Returns the number of timings recorded in the message history. Always returns 0 since this implementation does not track timings.
     *
     * @return 0, indicating no timings are recorded
     */
    @Override
    public int timings() {
        return 0; // No timings recorded
    }

    /**
     * Returns the timing for the specified index. Always returns -1 as no timings are recorded in this implementation.
     *
     * @param n The index of the timing (ignored)
     * @return -1, indicating no timing available
     */
    @Override
    public long timing(int n) {
        return -1; // No timing information available
    }

    /**
     * Returns the number of sources that processed the message. Always returns 0 as no source tracking is done.
     *
     * @return 0, indicating no sources processed the message
     */
    @Override
    public int sources() {
        return 0; // No sources recorded
    }

    /**
     * Returns the source ID for the specified index. Always returns -1 since source tracking is not done.
     *
     * @param n The index of the source ID (ignored)
     * @return -1, indicating no source ID available
     */
    @Override
    public int sourceId(int n) {
        return -1; // No source ID available
    }

    /**
     * Determines if the source IDs end with the specified array of source IDs. Always returns false as no source IDs are tracked.
     *
     * @param sourceIds Array of source IDs to check (ignored)
     * @return false, indicating the source IDs do not match
     */
    @Override
    public boolean sourceIdsEndsWith(int[] sourceIds) {
        return false; // No source IDs to compare
    }

    /**
     * Returns the source index for the specified index. Always returns -1 as source index tracking is not done.
     *
     * @param n The index of the source (ignored)
     * @return -1, indicating no source index available
     */
    @Override
    public long sourceIndex(int n) {
        return -1; // No source index available
    }

    /**
     * Resets the message history with the provided source ID and source index.
     * This implementation ignores the reset as no history is stored.
     *
     * @param sourceId    The source ID to reset (ignored)
     * @param sourceIndex The source index to reset (ignored)
     */
    @Override
    public void reset(int sourceId, long sourceIndex) {
        // No-op: no history is stored, so reset is ignored
    }

    /**
     * Resets the message history. This implementation performs no action as no history is stored.
     */
    public void reset() {
        // No-op: nothing to reset
    }

    /**
     * Returns the ID of the last source that processed the message. Always returns -1 as no source tracking is done.
     *
     * @return -1, indicating no last source ID available
     */
    @Override
    public int lastSourceId() {
        return -1; // No last source ID available
    }

    /**
     * Returns the index of the last source that processed the message. Always returns -1 as no source tracking is done.
     *
     * @return -1, indicating no last source index available
     */
    @Override
    public long lastSourceIndex() {
        return -1; // No last source index available
    }

    /**
     * Indicates if the message history has been modified. Always returns false as no history is stored or tracked.
     *
     * @return false, indicating the history is not dirty
     */
    @Override
    public boolean isDirty() {
        return false; // No history modifications to track
    }
}
