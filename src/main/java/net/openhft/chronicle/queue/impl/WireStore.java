/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue.impl;

import java.io.EOFException;
import java.util.concurrent.TimeoutException;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.ReferenceCounted;
import net.openhft.chronicle.queue.impl.single.ScanResult;
import net.openhft.chronicle.wire.Demarshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;

public interface WireStore extends ReferenceCounted, Demarshallable, WriteMarshallable {
    WireStore writePosition(long position);

    /**
     * @return an epoch offset as the number of number of milliseconds since January 1, 1970,
     * 00:00:00 GMT, if you set the epoch to the current time, then the cycle will be ZERO
     */
    long epoch();

    /**
     * @return the next writable position, Will be or-ed with ROLLED_BIT if it has rolled.
     */
    long writePosition();

    ScanResult moveToIndex(@NotNull Wire wire, long index, long timeoutMS) throws TimeoutException;

    @NotNull
    Bytes<Void> bytes();

    /**
     * Reverse look up an index for a position.
     *
     * @param position  of the start of the message
     * @param timeoutMS
     * @return index in this store.
     */
    long indexForPosition(Wire wire, long position, long timeoutMS) throws EOFException, TimeoutException;

    String dump();

    void lastAcknowledgedIndexReplicated(long lastAcknowledgedIndexReplicated);

    long lastAcknowledgedIndexReplicated();
}
