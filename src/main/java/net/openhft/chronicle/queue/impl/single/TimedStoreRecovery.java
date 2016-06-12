package net.openhft.chronicle.queue.impl.single;

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
        timeStamp = in.read(() -> "timeStamp").int64ForBinding(null);
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
                LOG.error("Unable to obtain the global lock in time, retrying");
                start = now;
            }
            Jvm.pause(1);
        }
    }

    void releaseLock(long tsEnd) {
        if (timeStamp.compareAndSwapValue(tsEnd, 0L))
            return;
        LOG.error("Another thread obtained the lock ??");
    }

    @Override
    public long recoverIndex2Index(LongValue index2Index, Callable<Long> action, long timeoutMS) throws UnrecoverableTimeoutException, EOFException {
        long tsEnd = acquireLock(timeoutMS);
        if (index2Index.getValue() == BinaryLongReference.LONG_NOT_COMPLETE) {
            LOG.warn("Rebuilding the index2index, resetting to 0");
            index2Index.setValue(0);
        } else {
            LOG.warn("The index2index value has changed, assuming it was recovered");
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
    public long recoverSecondaryAddress(LongArrayValues index2indexArr, int index2, Callable<Long> action, long timeoutMS) throws UnrecoverableTimeoutException, EOFException {
        long tsEnd = acquireLock(timeoutMS);
        if (index2indexArr.getValueAt(index2) == BinaryLongReference.LONG_NOT_COMPLETE) {
            LOG.warn("Rebuilding the index2index[" + index2 + "], resetting to 0");
            index2indexArr.setValueAt(index2, 0L);
        } else {
            LOG.warn("The index2index[" + index2 + "] value has changed, assuming it was recovered");
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
    public long recoverAndWriteHeader(Wire wire, int length, long timeoutMS) throws UnrecoverableTimeoutException {
        while (true) {
            LOG.warn("Unable to write a header at index: " + Long.toHexString(wire.headerNumber()) + " position: " + wire.bytes().writePosition());
            try {
                return wire.writeHeader(length, timeoutMS, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                LOG.warn("", e);
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
