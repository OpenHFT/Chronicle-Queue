/*
 * Copyright 2016-2020 chronicle.software
 *
 * https://chronicle.software
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

import net.openhft.chronicle.queue.impl.single.ScanResult;
import net.openhft.chronicle.wire.UnrecoverableTimeoutException;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.StreamCorruptedException;

public interface WireStore extends CommonStore, Closeable {

    /**
     * @param position the start of the last written excerpt to this cycle/store
     * @return this store
     */
    @NotNull
    WireStore writePosition(long position);

    /**
     * @return the start of the last written excerpt to this cycle/store
     */
    long writePosition();

    @Nullable
    ScanResult moveToIndexForRead(@NotNull ExcerptContext ec, long index);

    /**
     * Reverse look up an index for a position.
     *
     * @param ec       the wire of the bytes, to work with
     * @param position of the start of the message
     * @return index in this store.
     */
    long sequenceForPosition(ExcerptContext ec, long position, boolean inclusive)
            throws UnrecoverableTimeoutException, StreamCorruptedException;

    long lastSequenceNumber(ExcerptContext ec) throws StreamCorruptedException;

    void setPositionForSequenceNumber(final ExcerptContext ec, long sequenceNumber, long position) throws UnrecoverableTimeoutException, StreamCorruptedException;

    /**
     * @return true if EOF was written
     */
    boolean writeEOF(Wire wire, long timeoutMS);

    boolean indexable(long index);

    ScanResult linearScanTo(long index, long knownIndex, ExcerptContext ec, long knownAddress);

    long moveToEndForRead(@NotNull Wire w);

    void initIndex(Wire wire);

    String dumpHeader();

    int dataVersion();
}
