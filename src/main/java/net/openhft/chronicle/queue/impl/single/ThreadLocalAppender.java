/*
 * Copyright 2014-2020 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
