package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ExcerptAppender;

public class ThreadLocalAppender {

    /**
     * Returns a ExcerptAppender for the given ChronicleQueue that is local to the current Thread.
     * <p>
     * An Appender can be used to store new excerpts sequentially to the queue.
     * <p>
     * <b>
     * An Appender is <em>NOT thread-safe</em> and, in addition to that, confined to be used <em>by the creating thread only.</em>.
     * Sharing an Appender across threads is unsafe and will inevitably lead to errors and unspecified behaviour.
     * </b>
     * <p>
     * This method returns a {@link ThreadLocal} appender, so does not produce any garbage, hence it's safe to simply call
     * this method every time an appender is needed.
     *
     * @return Returns a ExcerptAppender for this ChronicleQueue that is local to the current Thread
     */
    public static ExcerptAppender acquireThreadLocalAppender(SingleChronicleQueue queue) {
        return queue.acquireThreadLocalAppender(queue);
    }
}
