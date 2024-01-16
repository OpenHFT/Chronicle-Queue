package net.openhft.chronicle.queue.internal.util;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.ThreadLocalAppender;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Registry managing thread local instances of {@link ThreadLocalAppender}s.
 */
public class InternalThreadLocalAppenderRegistry {

    private static final Map<ChronicleQueue, ThreadLocalAppender> INSTANCES = new ConcurrentHashMap<>();

    /**
     * Acquire a thread local appender related to the current queue. The supplier will be used to create the appender
     * instance in the thread local.
     */
    public static ExcerptAppender acquireThreadLocalAppender(ChronicleQueue queue, Supplier<ExcerptAppender> excerptAppenderSupplier) {
        ThreadLocalAppender appender = INSTANCES.computeIfAbsent(queue, q -> new ThreadLocalAppender(excerptAppenderSupplier));
        return appender.get();
    }

}
