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
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.queue.batch.BatchAppender;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;

/**
 * <p>The component that facilitates sequentially writing data to a {@link ChronicleQueue}.</p>
 * <p><b>NOTE:</b> Appenders are NOT thread-safe, sharing the Appender between threads will lead to errors and unpredictable behaviour.</p>
 *
 * @author peter.lawrey
 */
public interface ExcerptAppender extends ExcerptCommon<ExcerptAppender>, MarshallableOut {

    /**
     * @param bytes to write to excerpt.
     */
    void writeBytes(@NotNull BytesStore bytes) throws UnrecoverableTimeoutException;

    default void writeBytes(@NotNull Bytes bytes) throws UnrecoverableTimeoutException {
        writeBytes((BytesStore) bytes);
    }

    /**
     * @return the index last written, this index includes the cycle and the sequence number
     * @throws IllegalStateException if no index is available
     */
    long lastIndexAppended();

    /**
     * @return the cycle this appender is on, usually with chronicle-queue each cycle will have its
     * own unique data file to store the excerpt
     */
    int cycle();

    /**
     * We suggest this code is called from a background thread [ not you main
     * business thread ], it must be called from the same thread that created it, as the call to
     * pretouch() is not thread safe. For example :
     * <p>
     * newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> queue.acquireAppender().pretouch(), 0, 1, TimeUnit.SECONDS);
     */
    default void pretouch() {
    }

    /**
     * Enable padding if near the end of a cache line, pad it so a following 4-byte int value will
     * not split a cache line.
     */
    void padToCacheAlign(Padding padToCacheAlign);

    @Override
    @NotNull
    Padding padToCacheAlignMode();

    /**
     * A task that will be run if a WeakReference referring this appender is registered with a clean-up task.
     *
     * @return Task to release any associated resources
     */
    default Runnable getCloserJob() {
        return () -> {
        };
    }

    @NotNull
    default <T> T methodWriter(@NotNull Class<T> tClass, Class... additional) {
        return queue().methodWriter(tClass, additional);
    }

    @NotNull
    default <T> VanillaMethodWriterBuilder<T> methodWriterBuilder(@NotNull Class<T> tClass) {
        return queue().methodWriterBuilder(tClass);
    }

    /**
     * Obtain the raw wire for low level direct access.
     *
     * @return the raw wire.
     */
    Wire wire();

    /**
     * @param timeoutMS
     * @param batchAppender
     * @return the number of messages written in call the batches
     */
    @Deprecated
    long batchAppend(final int timeoutMS, BatchAppender batchAppender);

}
