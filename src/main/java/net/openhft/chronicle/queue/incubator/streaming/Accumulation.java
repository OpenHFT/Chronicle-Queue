package net.openhft.chronicle.queue.incubator.streaming;

import net.openhft.chronicle.core.annotation.NonNegative;
import net.openhft.chronicle.queue.AppenderListener;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.internal.streaming.AccumulationUtil;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static net.openhft.chronicle.core.util.ObjectUtils.requireNonNull;

public interface Accumulation<T> extends AppenderListener {

    /**
     * Consumes an excerpt from the provided {@code wire} at the index at the provided {@code index}.
     * <p>
     * If this method throws an Exception, it is relayed to the call site.
     * Therefore, care should be taken to minimise the probability of throwing Exceptions.
     * <p>
     * If this method is referenced as an {@link AppenderListener} then the Accumulation must be
     * thread-safe.
     **/
    void onExcerpt(@NotNull Wire wire, @NonNegative long index);

    /**
     * Returns a view of the underlying accumulation.
     *
     * @return accumulation view.
     */
    @NotNull
    T accumulation();

    /**
     * Accepts the input of the provided {@code tailer } and accumulates (folds) the contents of it
     * into this Accumulation returning the last seen index or -1 if no index was seen.
     * <p>
     * This method can be used to initialise the Accumulation before appending new values.
     * <p>
     * It is the responsibility of the caller to make sure no simultaneous appenders are using
     * this Accumulation during the entire fold operation.
     *
     * @param tailer to fold (accumulate) from
     * @return the last index seen or -1 if no index was seen
     * @throws NullPointerException if the provided {@code tailer} is {@code null}
     */
    default long accept(@NotNull ExcerptTailer tailer) {
        Objects.requireNonNull(tailer);
        return AccumulationUtil.accept(this, tailer);
    }
}