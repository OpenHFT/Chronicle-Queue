package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.values.LongArrayValues;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.wire.AbstractMarshallable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

/**
 * Created by peter on 22/05/16.
 */
public class SimpleStoreRecovery extends AbstractMarshallable implements StoreRecovery {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleStoreRecovery.class);

    @Override
    public long recoverIndex2Index(LongValue index2Index, Callable<Long> action, long timeoutMS) throws TimeoutException, EOFException {
        LOG.warn("Rebuilding the index2index");
        index2Index.setValue(0);
        try {
            return action.call();
        } catch (Exception e) {
            throw Jvm.rethrow(e);
        }
    }

    @Override
    public long recoverSecondaryAddress(LongArrayValues index2indexArr, int index2, Callable<Long> action, long timeoutMS) throws TimeoutException, EOFException {
        LOG.warn("Timed out trying to get index2index[" + index2 + "]");
        index2indexArr.setValueAt(index2, 0L);
        try {
            return action.call();
        } catch (Exception e) {
            throw Jvm.rethrow(e);
        }
    }
}
