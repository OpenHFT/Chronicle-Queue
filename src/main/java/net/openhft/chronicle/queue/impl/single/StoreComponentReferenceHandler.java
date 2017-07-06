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

public enum StoreComponentReferenceHandler {
    ;
    public static final String THREAD_NAME = "queue-thread-local-cleaner-daemon";

    private static final Logger LOGGER = LoggerFactory.getLogger(StoreComponentReferenceHandler.class);
    private static final ReferenceQueue<ExcerptAppender> EXPIRED_THREAD_LOCAL_APPENDERS_QUEUE = new ReferenceQueue<>();
    private static final ReferenceQueue<SingleChronicleQueueExcerpts.StoreTailer>
            EXPIRED_THREAD_LOCAL_TAILERS_QUEUE = new ReferenceQueue<>();
    private static final ExecutorService THREAD_LOCAL_CLEANER_EXECUTOR_SERVICE =
            Executors.newSingleThreadExecutor(r -> {
                final Thread thread = new Thread(r);
                thread.setDaemon(true);
                thread.setName(THREAD_NAME);
                return thread;
            });
    private static final ConcurrentMap<Reference<?>, Runnable> CLOSE_ACTIONS = new ConcurrentHashMap<>();
    private static final boolean SHOULD_RELEASE_RESOURCES =
            Boolean.valueOf(System.getProperty("chronicle.queue.release.weakRef.resources",
                    Boolean.TRUE.toString()));

    static {
        THREAD_LOCAL_CLEANER_EXECUTOR_SERVICE.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                processReferenceQueue(EXPIRED_THREAD_LOCAL_APPENDERS_QUEUE);
                processReferenceQueue(EXPIRED_THREAD_LOCAL_TAILERS_QUEUE);
            }
        });
    }

    static ReferenceQueue<ExcerptAppender> appenderQueue() {
        return EXPIRED_THREAD_LOCAL_APPENDERS_QUEUE;
    }

    static ReferenceQueue<SingleChronicleQueueExcerpts.StoreTailer> tailerQueue() {
        return EXPIRED_THREAD_LOCAL_TAILERS_QUEUE;
    }

    static <T> void register(final Reference<T> reference, final Runnable cleanupJob) {
        CLOSE_ACTIONS.put(reference, cleanupJob);
    }

    private static void processReferenceQueue(final ReferenceQueue<?> referenceQueue) {
        try {
            final Reference<?> reference = referenceQueue.remove(1000L);

            if (reference != null && reference.get() == null) {
                final Runnable closeAction = CLOSE_ACTIONS.remove(reference);
                if (closeAction != null && SHOULD_RELEASE_RESOURCES) {
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
}