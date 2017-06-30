package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ExcerptAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

enum StoreAppenderReferenceHandler {
    ;
    private static final Logger LOGGER = LoggerFactory.getLogger(StoreAppenderReferenceHandler.class);

    private static final ReferenceQueue<ExcerptAppender> EXPIRED_THREAD_LOCAL_APPENDERS_QUEUE = new ReferenceQueue<>();
    private static final ExecutorService THREAD_LOCAL_CLEANER_EXECUTOR_SERVICE =
            Executors.newSingleThreadExecutor(r -> {
                final Thread thread = new Thread(r);
                thread.setDaemon(true);
                thread.setName("queue-thread-local-cleaner-daemon");
                return thread;
            });
    private static ConcurrentMap<Reference<?>, Runnable> CLOSE_ACTIONS = new ConcurrentHashMap<>();

    static {
        THREAD_LOCAL_CLEANER_EXECUTOR_SERVICE.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    final Reference<?> reference = EXPIRED_THREAD_LOCAL_APPENDERS_QUEUE.remove(1000L);

                    if (reference != null && reference.get() == null) {
                        final Runnable closeAction = CLOSE_ACTIONS.remove(reference);
                        if (closeAction != null) {
                            closeAction.run();
                        }
                    }
                } catch (InterruptedException e) {
                    LOGGER.warn("Thread-local cleaner thread was interrupted. Exiting.");
                    Thread.currentThread().interrupt();
                } catch (RuntimeException e) {
                    LOGGER.warn("Error occurred attempting to close ExcerptAppender.", e);
                }
            }
        });
    }

    static ReferenceQueue<ExcerptAppender> appenderQueue() {
        return EXPIRED_THREAD_LOCAL_APPENDERS_QUEUE;
    }

    static <T> void register(final Reference<T> reference, final Runnable cleanupJob) {
        CLOSE_ACTIONS.put(reference, cleanupJob);
    }
}