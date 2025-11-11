/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.util.ThrowingBiFunction;
import org.jetbrains.annotations.NotNull;

import java.io.File;

/**
 * The {@code AsyncBufferCreator} interface defines a contract for creating buffers
 * for use in asynchronous mode. This functionality is available as an enterprise feature.
 *
 * It extends {@link ThrowingBiFunction}, allowing the creation of {@link BytesStore} instances
 * with a given size, maximum readers, and file backing.
 */
public interface AsyncBufferCreator extends ThrowingBiFunction<Long, Integer, BytesStore<?, ?>, Exception> {

    /**
     * This method is not supported in this interface and will always throw an {@link UnsupportedOperationException}.
     * Use the {@link #create(long, int, File)} method instead.
     *
     * @param size       The size of the buffer to be created.
     * @param maxReaders The maximum number of readers supported by the buffer.
     * @return Throws an {@link UnsupportedOperationException}.
     * @throws UnsupportedOperationException Always thrown to indicate the method should not be used.
     */
    @Override
    default @NotNull BytesStore<?, ?> apply(Long size, Integer maxReaders) throws Exception {
        throw new UnsupportedOperationException("Call the create function instead");
    }

    /**
     * Creates a {@link BytesStore} with the given size, maximum readers, and associated file for asynchronous operations.
     *
     * @param size       The size of the buffer to create.
     * @param maxReaders The maximum number of readers that can access the buffer.
     * @param file       The file associated with the buffer for storage.
     * @return A {@link BytesStore} instance configured for asynchronous operations.
     * @throws Exception If any error occurs during buffer creation.
     */
    @NotNull BytesStore<?, ?> create(long size, int maxReaders, File file) throws Exception;
}
