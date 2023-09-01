package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.util.ThrowingBiFunction;
import org.jetbrains.annotations.NotNull;

import java.io.File;

/**
 * Creates a buffer for use in async mode. This is an enterprise feature.
 * See queue
 */
public interface AsyncBufferCreator extends ThrowingBiFunction<Long, Integer, BytesStore, Exception> {

    @Override
    default @NotNull BytesStore apply(Long size, Integer maxReaders) throws Exception {
        throw new UnsupportedOperationException("Call the create function instead");
    }

    @NotNull BytesStore create(long size, int maxReaders, File file) throws Exception;
}
