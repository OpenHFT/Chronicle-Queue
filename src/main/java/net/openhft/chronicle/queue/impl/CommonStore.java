/*
 * Copyright 2016-2022 chronicle.software
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
 * be serialized and deserialized using Chronicle's wire formats.</p>
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
