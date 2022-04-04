package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;

/**
 * A listener interface for receiving events when excerpt
 * are durably persisted to a queue.
 * <p>
 * Implementations of this interface must be thread-safe as further discussed
 * under {@link #onExcerpt(Wire, long)}.
 *
 * @see SingleChronicleQueueBuilder#appenderListener(AppenderListener)
 */
@FunctionalInterface
public interface AppenderListener {

    /**
     * Invoked after an excerpt has been durably persisted to a queue.
     * <p>
     * The Thread that invokes this method is unspecified and may change, even
     * from invocation to invocation. This means implementations must ensure thread-safety
     * to guarantee correct behaviour. In particular, <em>it is an error to assume
     * the appending Thread will always be used do invoke this method</em>.
     * <p>
     * If this method throws an Exception, it is relayed to the call site.
     * Therefore, care should be taken to minimise the probability of throwing Exceptions.
     * <p>
     * It is imperative that actions performed by the method are as performant
     * as possible as any delay incurred by the invocation of this method
     * will carry over to the appender used to actually persist the message
     * (i.e. both for synchronous and asynchronous appenders actually storing messages).
     * <p>
     * No promise is given as to when this method is invoked. However, eventually
     * the method will be called for each excerpt persisted to the queue.
     * <p>
     * No promise is given as to the order in which invocations are made of this method.
     *
     * @param wire representing access to the excerpt that was stored (non-null).
     * @param index in the queue where the except was placed (non-negative)
     */
    void onExcerpt(@NotNull Wire wire, long index);
}
