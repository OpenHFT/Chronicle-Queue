package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.annotation.NonNegative;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;

import static net.openhft.chronicle.core.util.ObjectUtils.requireNonNull;

/**
 * A listener interface for receiving events when excerpts are encountered.
 */
@FunctionalInterface
public interface ExcerptListener {

    /**
     * Invoked per each encountered excerpt.
     * <p>
     * If this method throws an Exception, it is relayed to the call site.
     * Therefore, care should be taken to minimise the probability of throwing Exceptions.
     *
     * @param wire  representing access to the excerpt that was stored (non-null).
     * @param index in the queue where the except was placed (non-negative)
     */
    void onExcerpt(@NotNull Wire wire, @NonNegative long index);

    /**
     * Returns a composed ExcerptListener that first accepts excerpts to this ExcerptListener,
     * and then accepts excerpts to the {@code after} ExcerptListener.
     * If execution of either listener throws an exception, it is relayed to
     * the caller of the composed ExcerptListener.
     * <p>
     * Care should be taken to only create composed listeners that are performant.
     *
     * @param after the ExcerptListener to accept excerpts after this ExcerptListener
     * @return a composed ExcerptListener
     * @throws NullPointerException if {@code after} is {@code null }
     */
    @NotNull
    default ExcerptListener andThen(@NotNull final ExcerptListener after) {
        requireNonNull(after);
        return ((wire, index) -> {
            final long readPosition = wire.bytes().readPosition();
            this.onExcerpt(wire, index);
            // Rewind the wire to allow replaying of messages
            wire.bytes().readPosition(readPosition);
            after.onExcerpt(wire, index);
        });
    }

}