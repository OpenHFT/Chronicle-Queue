/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;

/**
 * Internal implementation class that enables the use of a thread local appender. Please ensure that you carefully read
 * the javadoc and understand the lifecycle of this thread-local appender before use.
 */
public final class ThreadLocalAppender {

    private ThreadLocalAppender() {
        // Intentional no-op
    }

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
     * @throws IllegalArgumentException if the queue it is passed is not an instance of {@link SingleChronicleQueue}
     */
    public static ExcerptAppender acquireThreadLocalAppender(ChronicleQueue queue) {
        if (!(queue instanceof SingleChronicleQueue)) {
            throw new IllegalArgumentException("acquireThreadLocalAppender only accepts instances of SingleChronicleQueue");
        }
        SingleChronicleQueue singleChronicleQueue = (SingleChronicleQueue) queue;
        return singleChronicleQueue.acquireThreadLocalAppender(singleChronicleQueue);
    }
}
