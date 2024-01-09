package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ExcerptAppender;

public class ThreadLocalAppender {

    /**
     * A thread local appender for the SingleChronicleQueue, this will give you the same thread local instance per thread.
     *
     * @param queue the SingleChronicleQueue
     * @return a thread local appender
     */
    public static ExcerptAppender acquireThreadLocalAppender(SingleChronicleQueue queue) {
        return queue.acquireThreadLocalAppender(queue);
    }
}
