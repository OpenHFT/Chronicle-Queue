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
    long recoverIndex2Index(LongValue index2Index, Callable<Long> action, long timeoutMS) throws UnrecoverableTimeoutException, EOFException;

    long recoverSecondaryAddress(LongArrayValues index2indexArr, int index2, Callable<Long> action, long timeoutMS) throws UnrecoverableTimeoutException, EOFException;

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
