package net.openhft.chronicle.queue.impl.ringbuffer;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.NativeBytesStore;
import net.openhft.chronicle.core.Jvm;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

/**
 * @author Rob Austin.
 */
public interface BytesRingBuffer {

    Logger LOG = LoggerFactory.getLogger(BytesRingBuffer.class);

    void clear();

    long minNumberOfWriteBytesRemaining();

    long capacity();

    boolean offer(@NotNull Bytes bytes0) throws InterruptedException;

    boolean read(@NotNull Bytes using) throws
            InterruptedException,
            IllegalStateException;

    long numberOfReadsSinceLastCall();

    long numberOfWritesSinceLastCall();

    long maxCopyTimeSinceLastCall();


    static BytesRingBuffer newInstance(NativeBytesStore<Void> bytesStore) {
        try {
            final Class<BytesRingBuffer> aClass = clazz();
            final Constructor<BytesRingBuffer> constructor = aClass.getDeclaredConstructor(NativeBytesStore.class);
            return constructor.newInstance(bytesStore);

        } catch (Exception e) {
            LOG.error("This is a a commercial feature, please contact " +
                    "sales@higherfrequencytrading.com to unlock this feature.");

            throw Jvm.rethrow(e);
        }
    }

    static Class<BytesRingBuffer> clazz() throws ClassNotFoundException {
        return (Class<BytesRingBuffer>) BytesRingBuffer.class.forName(
                "net.openhft.chronicle.queue.enterprise.EnterpriseBytesRingBuffer");
    }


    static long sizeFor(long cacacity) {
        try {
            final Method sizeFor = clazz().getMethod("sizeFor", String.class);
            return (long) sizeFor.invoke(null, cacacity);
        } catch (Exception e) {
            LOG.error("This is a a commercial feature, please contact " +
                    "sales@higherfrequencytrading.com to unlock this feature.");

            throw Jvm.rethrow(e);
        }

    }

}
