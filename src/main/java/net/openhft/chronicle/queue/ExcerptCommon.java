/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.SingleThreadedChecked;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;

/**
 * The ExcerptCommon is common to both ExcerptAppender
 * and ExcerptTailer.
 *
 * @param <E> the concrete Excerpt type returned for fluent chaining
 */
public interface ExcerptCommon<E extends ExcerptCommon<E>> extends Closeable, SingleThreadedChecked {

    /**
     * Returns the source id of the backing ChronicleQueue
     * to which this ExcerptCommon is attached to.
     *
     * @return the source id of the backing ChronicleQueue
     * @see ChronicleQueue#sourceId()
     */
    int sourceId();

    /**
     * Returns the backing ChronicleQueue to which this
     * ExcerptCommon is attached to.
     *
     * @return the backing ChronicleQueue to which this
     * ExcerptCommon is attached to
     */
    @NotNull
    ChronicleQueue queue();

    /**
     * Returns the current file being worked on, or {@code null} if the tailer/appender has not loaded a file yet.
     *
     * @return the current file being worked on or null if not known.
     */
    @Nullable
    default File currentFile() {
        return null;
    }

    /**
     * Performs a sync up to the point the Appender has written or Tailer has read, if supported.
     */
    default void sync() {
    }
}
