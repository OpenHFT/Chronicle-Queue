package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.values.LongArrayValues;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.wire.WriteMarshallable;

import java.io.EOFException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

/**
 * Created by peter on 21/05/16.
 */
public interface StoreRecovery extends WriteMarshallable {
    long recoverIndex2Index(LongValue index2Index, Callable<Long> action, long timeoutMS) throws TimeoutException, EOFException;

    long recoverSecondaryAddress(LongArrayValues index2indexArr, int index2, Callable<Long> action, long timeoutMS) throws TimeoutException, EOFException;
}
