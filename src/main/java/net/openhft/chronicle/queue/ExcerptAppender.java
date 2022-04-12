/*
 * Copyright 2016-2020 chronicle.software
 *
 * https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
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
import net.openhft.chronicle.wire.MarshallableOut;
import net.openhft.chronicle.wire.UnrecoverableTimeoutException;
import net.openhft.chronicle.wire.VanillaMethodWriterBuilder;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * <p>The component that facilitates sequentially writing data to a {@link ChronicleQueue}.</p>
 * <p><b>NOTE:</b> Appenders are NOT thread-safe, sharing the Appender between threads will lead to errors and unpredictable behaviour.</p>
 */
public interface ExcerptAppender extends ExcerptCommon<ExcerptAppender>, MarshallableOut {

    /**
     * Writes (i.e. appends) the provided {@code bytes} to the queue.
     *
     * @param bytes to write to excerpt.
     * @throws UnrecoverableTimeoutException if the operation times out.
     */
    void writeBytes(@NotNull BytesStore bytes);

    /**
     * Writes (i.e. appends) the provided {@code bytes} to the queue.
     *
     * @param bytes to write to excerpt.
     * @throws UnrecoverableTimeoutException if the operation times out.
     */
    default void writeBytes(@NotNull Bytes<?> bytes) {
        writeBytes((BytesStore) bytes);
    }

    /**
     * Returns the index last written.
     * <p>
     * The index includes the cycle and the sequence number.
     *
     * @return the index last written
     * @throws IllegalStateException if no index is available
     */
    long lastIndexAppended();

    /**
     * Returns the cycle this appender is on.
     * <p>
     * Usually with chronicle-queue each cycle will have its
     * own unique data file to store the excerpts
     *
     * @return the cycle this appender is on
     */
    int cycle();

    /**
     * Pre-touches storage resources for the current queue so that appenders
     * may exhibit more predictable latencies.
     * <p>
     * Pre-touching involves accessing pages of files/memory that are likely accessed in a
     * near future and may also involve accessing/acquiring future cycle files.
     * <p>
     * We suggest this code is called from a background thread [ not you main
     * business thread ], it must be called from the same thread that created it, as the call to
     * pretouch() is not thread safe. For example :
     * <p>
     * <code>newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -&gt; queue.acquireAppender().pretouch(), 0, 1, TimeUnit.SECONDS);</code>
     * <p>
     * NOTE: This pretoucher is assumed to be called periodically at longer regular intervals such a 100 ms or 1 second.
     */
    default void pretouch() {
    }

    /**
     * Creates and returns a new writer proxy for the given interface {@code tclass} and the given {@code additional }
     * interfaces.
     * <p>
     * When methods are invoked on the returned T object, messages will be put in the queue.
     * <p>
     * <b>
     * Writers are NOT thread-safe. Sharing a Writer across threads will lead to errors and unpredictable behaviour.
     * </b>
     *
     * @param tClass     of the main interface to be implemented
     * @param additional interfaces to be implemented
     * @param <T>        type parameter of the main interface
     * @return a new proxy for the given interface {@code tclass} and the given {@code additional }
     * interfaces
     * @throws NullPointerException if any of the provided parameters are {@code null}.
     */
    @NotNull
    @Override
    default <T> T methodWriter(@NotNull Class<T> tClass, Class... additional) {
        return queue().methodWriter(tClass, additional);
    }

    /**
     * Creates and returns a new writer proxy for the given interface {@code tclass}.
     * <p>
     * When methods are invoked on the returned T object, messages will be put in the queue.
     * <p>
     * <b>
     * Writers are NOT thread-safe. Sharing a Writer across threads will lead to errors and unpredictable behaviour.
     * </b>
     *
     * @param tClass of the main interface to be implemented
     * @param <T>    type parameter of the main interface
     * @return a new proxy for the given interface {@code tclass}
     * @throws NullPointerException if the provided parameter is {@code null}.
     */
    @NotNull
    @Override
    default <T> VanillaMethodWriterBuilder<T> methodWriterBuilder(@NotNull Class<T> tClass) {
        return queue().methodWriterBuilder(tClass);
    }

    /**
     * Returns a raw wire for low level direct access.
     *
     * @return a raw wire for low level direct access
     */
    @Nullable
    Wire wire();

    /**
     * Ensure all already-rolled cq4 files are correctly ended with EOF
     * Used by replication sinks on startup to cover off any edge cases where the replicated EOF was not received/applied
     * Can also be used on any appender, but this is not currently done automatically
     */
    default void normaliseEOFs() {}
}
