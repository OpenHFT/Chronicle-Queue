/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.annotation.SingleThreaded;
import net.openhft.chronicle.wire.MarshallableOut;
import net.openhft.chronicle.wire.UnrecoverableTimeoutException;
import net.openhft.chronicle.wire.VanillaMethodWriterBuilder;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * <p>The component that facilitates sequentially writing data to a {@link ChronicleQueue}.
 * <p><b>NOTE:</b> Appenders are NOT thread-safe, sharing the Appender between threads will lead to errors and unpredictable behaviour.
 */
@SingleThreaded
public interface ExcerptAppender extends ExcerptCommon<ExcerptAppender>, MarshallableOut {

    /**
     * Writes (i.e. appends) the provided {@code bytes} to the queue.
     *
     * @param bytes to write to excerpt.
     * @throws UnrecoverableTimeoutException if the operation times out.
     */
    void writeBytes(@NotNull BytesStore<?, ?> bytes);

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
     * We suggest this code is called from a background thread [ not your main
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
     * {@inheritDoc}
     * <p>
     * For Queue, an output context is a roll cycle. Each appender's listener runs under the queue
     * write lock before that appender's first data document in a cycle. Queue resolves the actual
     * monotonic destination, including one permitted advance past EOF, before the callback. It is
     * not called for metadata, explicit-index writes or append-locked queues. Callback failure
     * poisons the current cycle until a later roll; it is never retried in place. A restarted
     * appender can write context again in an existing cycle. Context listeners are not supported
     * with double buffering, asynchronous output, encoding or encryption, and the caller retains
     * ownership of the listener.
     */
    @NotNull
    @Override
    default <T> ExcerptAppender contextListener(@NotNull Class<T> writerType,
                                                @NotNull MarshallableOut.ContextListener<? super T> listener) {
        throw new UnsupportedOperationException();
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
    default <T> T methodWriter(@NotNull Class<T> tClass, Class<?>... additional) {
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
     * Ensure all already-rolled cq4 files are correctly ended with EOF.
     * Used by replication sinks before publishing back-fill completion to cover cases where a
     * failed exact-index recovery left an older cycle open. The exact-index caller remains
     * responsible for retrying failed writes, invoking this operation successfully and excluding
     * archive/delete maintenance until both have completed. Queue does not persist a recovery-intent
     * marker, and appender construction or ordinary append is not a completion guarantee.
     */
    default void normaliseEOFs() {
        // Intentionally no-op: optional hook for ensuring EOF markers on rolled files
    }
}
