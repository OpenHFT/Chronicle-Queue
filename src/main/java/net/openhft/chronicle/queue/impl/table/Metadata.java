/*
 * Copyright 2014-2020 chronicle.software
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

package net.openhft.chronicle.queue.impl.table;

import net.openhft.chronicle.wire.Demarshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;

/**
 * The {@code Metadata} interface provides a structure for classes that handle metadata operations
 * within a Chronicle Queue. It extends {@link Demarshallable} and {@link WriteMarshallable} to support
 * both deserialization and serialization of metadata objects.
 */
public interface Metadata extends Demarshallable, WriteMarshallable {

    /**
     * Allows overriding of metadata fields with values from another {@code Metadata} object.
     * This is a default method and can be overridden by implementing classes if needed.
     *
     * @param metadata The metadata object to override from.
     * @param <T> The type of metadata.
     */
    default <T extends Metadata> void overrideFrom(T metadata) {
        // Default implementation does nothing.
    }

    /**
     * An enumeration representing a no-op metadata instance.
     * Typically used as a placeholder where no actual metadata is needed.
     */
    enum NoMeta implements Metadata {
        /**
         * Singleton instance representing the absence of metadata.
         */
        INSTANCE;

        /**
         * No-arg constructor for {@code NoMeta}.
         */
        NoMeta() {
            // No operation.
        }

        /**
         * Constructor for {@code NoMeta} that accepts a {@link WireIn} for deserialization.
         * This constructor is required for classes that implement {@link Demarshallable}.
         *
         * @param in The {@link WireIn} instance for deserialization.
         */
        @SuppressWarnings("unused")
        NoMeta(@NotNull WireIn in) {
            // No operation.
        }

        /**
         * Writes nothing to the provided {@link WireOut} as this is a no-op metadata implementation.
         *
         * @param wire The {@link WireOut} instance to which this metadata is written.
         */
        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
            // No operation, no data to write.
        }
    }
}
