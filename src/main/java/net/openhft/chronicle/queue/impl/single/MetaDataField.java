/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
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
