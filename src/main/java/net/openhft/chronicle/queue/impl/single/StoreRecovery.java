/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.values.LongArrayValues;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.wire.UnrecoverableTimeoutException;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.Nullable;

import java.io.EOFException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by peter on 21/05/16.
 */
public interface StoreRecovery extends WriteMarshallable {
    long recoverIndex2Index(LongValue index2Index, Callable<Long> action, long timeoutMS) throws UnrecoverableTimeoutException;

    long recoverSecondaryAddress(LongArrayValues index2indexArr, int index2, Callable<Long> action, long timeoutMS) throws UnrecoverableTimeoutException;

    default long writeHeader(Wire wire,
                             int length,
                             long timeoutMS,
                             @Nullable final LongValue lastPosition) throws EOFException, UnrecoverableTimeoutException {
        try {
            return wire.writeHeader(length, timeoutMS, TimeUnit.MILLISECONDS, lastPosition);
        } catch (TimeoutException e) {
            return recoverAndWriteHeader(wire, length, timeoutMS, lastPosition);
        }
    }

    long recoverAndWriteHeader(Wire wire, int length, long timeoutMS, final LongValue lastPosition) throws UnrecoverableTimeoutException;

    void writeEndOfWire(Wire wire, long timeoutMS) throws UnrecoverableTimeoutException;
}
