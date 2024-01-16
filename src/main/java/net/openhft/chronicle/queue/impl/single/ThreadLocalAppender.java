package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.threads.CleaningThreadLocal;
import net.openhft.chronicle.queue.ExcerptAppender;

import java.util.function.Supplier;

/**
 * Internal implementation class that enables the use of a thread local appender. Please ensure that you carefully read
 * the javadoc and understand the lifecycle of this thread-local appender before use.
 */
public final class ThreadLocalAppender {

    private final ThreadLocal<ExcerptAppender> threadLocalAppender;

    public ThreadLocalAppender(Supplier<ExcerptAppender> excerptAppenderSupplier) {
        this.threadLocalAppender = CleaningThreadLocal.withCloseQuietly(excerptAppenderSupplier);
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
    public ExcerptAppender get() {
        return threadLocalAppender.get();
    }

}
