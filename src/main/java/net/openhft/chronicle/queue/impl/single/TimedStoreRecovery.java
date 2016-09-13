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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.ref.BinaryLongReference;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.values.LongArrayValues;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by peter on 21/05/16.
 */
public class TimedStoreRecovery extends AbstractMarshallable implements StoreRecovery, Demarshallable {
    public static final StoreRecoveryFactory FACTORY = TimedStoreRecovery::new;
    private static final Logger LOG = LoggerFactory.getLogger(TimedStoreRecovery.class);
    private final LongValue timeStamp;

    @UsedViaReflection
    public TimedStoreRecovery(WireIn in) {
        timeStamp = in.read(() -> "timeStamp").int64ForBinding(in.newLongReference());
    }

    public TimedStoreRecovery(WireType wireType) {
        timeStamp = wireType.newLongReference().get();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut out) {
        out.write("timeStamp").int64forBinding(0);
    }

    long acquireLock(long timeoutMS) {
        long start = System.currentTimeMillis();
        while (true) {
            long now = System.currentTimeMillis();
            long ts = timeStamp.getVolatileValue();
            final long tsEnd = now + timeoutMS / 2;
            if (ts < now && timeStamp.compareAndSwapValue(ts, tsEnd))
                return tsEnd;
            if (now >= start + timeoutMS) {
                Jvm.warn().on(getClass(), "Unable to obtain the global lock in time, retrying");
                start = now;
            }
            Jvm.pause(1);
        }
    }

    void releaseLock(long tsEnd) {
        if (timeStamp.compareAndSwapValue(tsEnd, 0L))
            return;
        Jvm.warn().on(getClass(), "Another thread obtained the lock ??");
    }

    @Override
    public long recoverIndex2Index(LongValue index2Index, Callable<Long> action, long timeoutMS) throws UnrecoverableTimeoutException {
        long tsEnd = acquireLock(timeoutMS);
        if (index2Index.getValue() == BinaryLongReference.LONG_NOT_COMPLETE) {
            Jvm.warn().on(getClass(), "Rebuilding the index2index, resetting to 0");
            index2Index.setValue(0);
        } else {
            Jvm.warn().on(getClass(), "The index2index value has changed, assuming it was recovered");
        }
        try {
            return action.call();
        } catch (Exception e) {
            throw Jvm.rethrow(e);
        } finally {
            releaseLock(tsEnd);
        }
    }

    @Override
    public long recoverSecondaryAddress(LongArrayValues index2indexArr, int index2, Callable<Long> action, long timeoutMS) throws UnrecoverableTimeoutException {
        long tsEnd = acquireLock(timeoutMS);
        if (index2indexArr.getValueAt(index2) == BinaryLongReference.LONG_NOT_COMPLETE) {
            Jvm.warn().on(getClass(), "Rebuilding the index2index[" + index2 + "], resetting to 0");
            index2indexArr.setValueAt(index2, 0L);
        } else {
            Jvm.warn().on(getClass(), "The index2index[" + index2 + "] value has changed, assuming it was recovered");
        }

        try {
            return action.call();
        } catch (Exception e) {
            throw Jvm.rethrow(e);
        } finally {
            releaseLock(tsEnd);
        }
    }

    @Override
    public long recoverAndWriteHeader(Wire wire, int length, long timeoutMS, final LongValue lastPosition) throws UnrecoverableTimeoutException {
        Bytes<?> bytes = wire.bytes();
        while (true) {
            long offset = bytes.writePosition();
            int num = bytes.readInt(offset);
            if (Wires.isNotComplete(num) && bytes.compareAndSwapInt(offset, num, 0)) {
                Jvm.warn().on(getClass(), "Unable to write a header at index: " + Long.toHexString(wire.headerNumber()) + " position: " + offset + " resetting");
            } else {
                Jvm.warn().on(getClass(), "Unable to write a header at index: " + Long.toHexString(wire.headerNumber()) + " position: " + offset + " unable to reset.");
            }
            try {
                return wire.writeHeader(length, timeoutMS, TimeUnit.MILLISECONDS, lastPosition);
            } catch (TimeoutException e) {
                Jvm.warn().on(getClass(), e);
            } catch (EOFException e) {
                throw new AssertionError(e);
            }
        }
    }

    @Override
    public void writeEndOfWire(Wire wire, long timeoutMS) throws UnrecoverableTimeoutException {
        throw new UnsupportedOperationException();
    }
}
