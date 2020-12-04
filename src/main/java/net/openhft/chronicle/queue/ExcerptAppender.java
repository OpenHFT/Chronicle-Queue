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
import net.openhft.chronicle.queue.batch.BatchAppender;
import net.openhft.chronicle.wire.MarshallableOut;
import net.openhft.chronicle.wire.UnrecoverableTimeoutException;
import net.openhft.chronicle.wire.VanillaMethodWriterBuilder;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;

/**
 * <p>The component that facilitates sequentially writing data to a {@link ChronicleQueue}.</p>
 * <p><b>NOTE:</b> Appenders are NOT thread-safe, sharing the Appender between threads will lead to errors and unpredictable behaviour.</p>
 */
public interface ExcerptAppender extends ExcerptCommon<ExcerptAppender>, MarshallableOut {

    /**
     * Writes (i.e. appends) the provided {@code bytes} to the queue.
     *
     * @param bytes to write to excerpt.
     */
    void writeBytes(@NotNull BytesStore bytes) throws UnrecoverableTimeoutException;

    /**
     * Writes (i.e. appends) the provided {@code bytes} to the queue.
     *
     * @param bytes to write to excerpt.
     */
    default void writeBytes(@NotNull Bytes bytes) throws UnrecoverableTimeoutException {
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
    default <T> VanillaMethodWriterBuilder<T> methodWriterBuilder(@NotNull Class<T> tClass) {
        return queue().methodWriterBuilder(tClass);
    }

    /**
     * Returns a raw wire for low level direct access.
     *
     * @return a raw wire for low level direct access
     */
    Wire wire();

    /**
     * Appends a number of excerpts in a single batch operation.
     *
     * @param timeoutMS     time out in miliseconds
     * @param batchAppender to apply for the batch append
     * @return the number of messages written in call the batches
     * @see BatchAppender
     * @deprecated This method is not recommended unless you really
     * know what you are doing. Misuse of this API could corrupt your data or even
     * worst cause the JVM or your application to crash.
     */
    @Deprecated(/* to be removed in x.22 */)
    long batchAppend(int timeoutMS, BatchAppender batchAppender);

}
