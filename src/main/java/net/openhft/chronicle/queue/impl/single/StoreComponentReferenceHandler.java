package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.wire.Wire;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public enum StoreComponentReferenceHandler implements Closeable {
    INSTANCE;
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
    private static final Queue<Wire> WIRES_TO_RELEASE = new ConcurrentLinkedQueue<>();
    private static final ConcurrentMap<Reference<?>, Runnable> CLOSE_ACTIONS = new ConcurrentHashMap<>();
    private static final boolean SHOULD_RELEASE_RESOURCES =
            Boolean.valueOf(System.getProperty("chronicle.queue.release.weakRef.resources",
                    Boolean.TRUE.toString()));
    private static final int MAX_BATCH_SIZE =
            Integer.getInteger("chronicle.queue.release.weakRef.maxBatch", 10_000);
    private static final AtomicBoolean MAX_BATCH_WARNING_LOGGED = new AtomicBoolean(false);

    static {
        THREAD_LOCAL_CLEANER_EXECUTOR_SERVICE.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                boolean workDone = processReferenceQueue(EXPIRED_THREAD_LOCAL_APPENDERS_QUEUE);
                workDone |= processReferenceQueue(EXPIRED_THREAD_LOCAL_TAILERS_QUEUE);
                workDone |= processWireQueue();

                if (!workDone) {
                    LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1L));
                }
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

    static void queueForRelease(final Wire wire) {
        WIRES_TO_RELEASE.add(wire);
    }

    private static boolean processWireQueue() {
        Wire wireToRelease;
        boolean released = false;
        while ((wireToRelease = WIRES_TO_RELEASE.poll()) != null) {
            try {
                released = true;
                wireToRelease.bytes().release();
            } catch (IllegalStateException e) {
                // ignore this - resource may have already been released by explicit close() operation
            } catch (Throwable t) {
                LOGGER.warn("Failed to release wire bytes", t);
            }
        }

        return released;
    }

    private static boolean processReferenceQueue(final ReferenceQueue<?> referenceQueue) {
        int processedCount = 0;
        try {
            Reference<?> reference;

            while ((reference = referenceQueue.poll()) != null) {
                if (processedCount++ == MAX_BATCH_SIZE) {
                    if (!MAX_BATCH_WARNING_LOGGED.get()) {
                        MAX_BATCH_WARNING_LOGGED.set(true);
                        LOGGER.warn("Weak ref queue processed {} entries, consider increasing max batch size" +
                                " via -Dchronicle.queue.release.weakRef.maxBatch", MAX_BATCH_SIZE);
                    }
                    return true;
                }
                if (reference.get() == null) {
                    final Runnable closeAction = CLOSE_ACTIONS.remove(reference);
                    if (closeAction != null && SHOULD_RELEASE_RESOURCES) {
                        closeAction.run();
                    }
                }
            }
        } catch (RuntimeException e) {
            LOGGER.warn("Error occurred attempting to close ExcerptAppender.", e);
        }
        return processedCount != 0;
    }

    @Override
    public void close() {
        THREAD_LOCAL_CLEANER_EXECUTOR_SERVICE.shutdownNow();
        try {
            THREAD_LOCAL_CLEANER_EXECUTOR_SERVICE.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException ignore) {

        }
    }
}