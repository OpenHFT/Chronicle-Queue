package net.openhft.chronicle.queue.incubator.streaming;

import net.openhft.chronicle.core.annotation.NonNegative;
import net.openhft.chronicle.queue.AppenderListener;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.internal.streaming.ReductionUtil;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

import static net.openhft.chronicle.core.util.ObjectUtils.requireNonNull;

public interface Reduction<T> extends AppenderListener {

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
     * Returns a view of the underlying reduction.
     *
     * @return accumulation view.
     */
    @NotNull
    T reduction();

    /**
     * Accepts the input of the provided {@code tailer } and reduces (folds) the contents of it
     * into this Reduction returning the last seen index or -1 if no index was seen.
     * <p>
     * This method can be used to initialise a Reduction before appending new values.
     * <p>
     * It is the responsibility of the caller to make sure no simultaneous appenders are using
     * this Reduction during the entire fold operation.
     *
     * @param tailer to reduce (fold) from
     * @return the last index seen or -1 if no index was seen
     * @throws NullPointerException if the provided {@code tailer} is {@code null}
     */
    default long accept(@NotNull final ExcerptTailer tailer) {
        requireNonNull(tailer);
        return ReductionUtil.accept(this, tailer);
    }
}