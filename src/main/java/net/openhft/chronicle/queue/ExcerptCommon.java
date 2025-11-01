/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
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
     * @return the current file being worked on or null if not known.
     */
    @Nullable
    default File currentFile() {
        return null;
    }

    /**
     * Performa sync up to the point the Appender has written or Tailer has read, if supported.
     */
    default void sync() {
    }
}
