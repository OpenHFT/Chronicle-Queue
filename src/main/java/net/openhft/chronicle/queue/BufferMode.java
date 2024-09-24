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

package net.openhft.chronicle.queue;

/**
 * Enum representing the different buffer modes that can be used within Chronicle Queue.
 * Each mode has a specific use case and behavior depending on the configuration.
 */
public enum BufferMode {
    /**
     * The default buffer mode.
     * No additional buffering or special handling is applied.
     */
    None,    // Default mode, no buffering

    /**
     * Buffer mode used in conjunction with encryption.
     * Data is copied into a buffer before being processed.
     */
    Copy,    // Used when encryption is enabled to handle buffered copies

    /**
     * Buffer mode used for asynchronous processing.
     * This mode is specific to Chronicle Ring, an enterprise product.
     */
    Asynchronous   // Asynchronous buffering used by Chronicle Ring (enterprise feature)
}
