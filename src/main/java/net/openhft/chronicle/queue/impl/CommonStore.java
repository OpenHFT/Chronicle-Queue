/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.wire.Demarshallable;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;

/**
 * The {@code CommonStore} interface provides an abstraction for managing the underlying storage of a queue.
 * It includes methods for accessing the file associated with the store, retrieving the underlying byte storage,
 * and dumping the contents of the store in a specific wire format.
 *
 * <p>This interface extends both {@link Demarshallable} and {@link WriteMarshallable}, allowing the store to
 * be serialized and deserialized using Chronicle's wire formats.
 */
public interface CommonStore extends Demarshallable, WriteMarshallable {
    /**
     * Returns the file associated with this store, if any.
     *
     * @return the {@link File} object representing the store's file, or {@code null} if no file is associated.
     */
    @Nullable
    File file();

    /**
     * Provides access to the underlying {@link MappedBytes} for this store.
     *
     * @return the {@link MappedBytes} representing the byte storage for this store.
     */
    @NotNull
    MappedBytes bytes();

    /**
     * Dumps the contents of this store in the specified {@link WireType} format.
     *
     * @param wireType the {@link WireType} used for dumping the contents.
     * @return a {@link String} representing the dumped contents of the store.
     */
    String dump(WireType wireType);
}
