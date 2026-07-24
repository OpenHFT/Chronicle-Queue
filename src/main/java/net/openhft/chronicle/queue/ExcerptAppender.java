/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.annotation.SingleThreaded;
import net.openhft.chronicle.wire.MarshallableOut;
import net.openhft.chronicle.wire.MessageHistory;
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
     * Sets a listener to be called before this appender writes to a newly-created output context.
     * For Queue, the output context is a newly-created roll file.
     * <p>
     * The listener receives a preset method writer for {@code writerType}. Calls made on that
     * writer are appended before the write that triggered creation of that context. The listener
     * must write only through that supplied writer and must not re-enter the appender (for example
     * by opening its own writing document) during the callback. The supplied writer is scoped to the
     * callback; retaining it or using it after the callback returns is unsupported.
     * <p>
     * The listener fires only before the first ordinary data append into a new, empty roll file,
     * including first use of an empty queue; it is not limited to later clock-driven rolls.
     * It is not fired for metadata writes, for an append-locked (replication sink) queue, or for explicit
     * index writes ({@code writeBytes(index, ...)}) - injecting records there would bypass the
     * append lock or shift the caller's index.
     * <p>
     * With double buffering enabled, the callback may run when the buffered write is flushed; the
     * context records still precede the buffered data in the queue. A caller that uses
     * {@link net.openhft.chronicle.wire.DocumentContext#contextCount()} for progressive
     * context resends must not use double buffering; the final target cycle is selected when
     * the buffer is flushed, so buffered contexts reject context-count access.
     * <p>
     * The callback runs while the queue write lock is held. It must be allocation-light, must not
     * block, and must not perform slow I/O.
     * <p>
     * The supplied writer emits normal method-writer documents. Do not enable this listener on a
     * queue whose readers require one fixed raw payload format unless those readers explicitly
     * tolerate the context records.
     * <p>
     * A context record is advisory, not guaranteed session state: it is not written when appending
     * to an existing non-empty roll file, for example after restart or failover into the middle of a
     * roll.
     * <p>
     * Context records are synthetic and do not record {@link MessageHistory} by default. A listener
     * may write history explicitly if that context has a real causal history, but normal usage
     * assumes no history is written.
     * <p>
     * Context that must exist before the first appender write is application state and must be
     * written by the application as it is built; this listener is not a construction-time callback.
     * On first use, and on later new-roll callbacks, the listener can dump the current state and
     * resend any previously written context that new readers may rely on. If there is no context to
     * write for that callback, it may return without writing anything; Queue treats the context as
     * handled and does not write a placeholder record.
     * <p>
     * A low-latency resend can clear any local "already sent" assumptions first, then write the
     * missing context while one {@link net.openhft.chronicle.wire.DocumentContext} is held if the
     * supplied writer also exposes {@link net.openhft.chronicle.wire.DocumentWritten}. This is the
     * same method-writer pattern as creating a writer with
     * {@code methodWriter(EventType.class, DocumentWritten.class)} and making several writer calls
     * before closing the document.
     * <p>
     * This method must be called before the appender's first write attempt. If the listener also implements
     * {@link java.lang.AutoCloseable}, it is closed when this appender is closed, unless the same
     * instance is configured on the queue builder (then the queue owns it) or is shared with another
     * appender (then it is closed once, by the last appender to release it).
     *
     * @param writerType event interface type for the supplied method writer
     * @param listener   listener to call for new output contexts
     * @param <T>        event interface type
     * @return this appender
     * @throws IllegalStateException         if called after this appender has written
     * @throws UnsupportedOperationException from the default implementation - only appenders that
     *                                       support context listeners override this method
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
     * Ensure all already-rolled cq4 files are correctly ended with EOF
     * Used by replication sinks on startup to cover off any edge cases where the replicated EOF was not received/applied
     * Can also be used on any appender, but this is not currently done automatically
     */
    default void normaliseEOFs() {
        // Intentionally no-op: optional hook for ensuring EOF markers on rolled files
    }
}
