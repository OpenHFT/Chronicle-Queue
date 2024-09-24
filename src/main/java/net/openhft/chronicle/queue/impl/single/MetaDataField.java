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

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.wire.WireKey;
import org.jetbrains.annotations.Nullable;

/**
 * The {@code MetaDataField} enum represents various metadata fields used within a Chronicle Queue.
 * These fields are associated with metadata stored in queue files, such as wire types, write positions,
 * and replication-related information.
 * <p>
 * Each field may have a specific role in managing or accessing the queue metadata, and certain fields
 * may require a default value for proper queue functioning.
 */
public enum MetaDataField implements WireKey {
    wireType,
    writePosition,
    roll,
    indexing,
    lastAcknowledgedIndexReplicated,
    recovery,
    deltaCheckpointInterval,
    encodedSequence,
    lastIndexReplicated,
    sourceId,
    dataFormat,
    metadata;

    /**
     * This method throws an {@link IORuntimeException} indicating that the field requires a value to be provided,
     * as no default value is available.
     *
     * @return Throws an exception indicating that the field requires a value.
     * @throws IORuntimeException when a default value is requested for a field that doesn't support one.
     */
    @Nullable
    @Override
    public Object defaultValue() {
        throw new IORuntimeException("field " + name() + " required");
    }
}
