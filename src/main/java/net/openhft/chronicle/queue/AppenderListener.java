package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

/**
 * A listener interface for receiving events when excerpt
 * are durably persisted to a queue.
 * <p>
 *
 * @see SingleChronicleQueueBuilder#appenderListener(ExcerptListener)
 * @deprecated for removal in x.25. Use {@link ExcerptListener} instead.
 */
@FunctionalInterface
@Deprecated()
public interface AppenderListener extends ExcerptListener {
}