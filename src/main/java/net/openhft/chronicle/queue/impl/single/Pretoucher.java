package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.NewChunkListener;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.ClosedIllegalStateException;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.core.time.TimeProvider;

import java.util.function.IntConsumer;

/**
 * A class designed to be called from a long-lived thread.
 * <p>
 * Upon invocation of the {@code execute()} method, this object will pre-touch pages in the supplied queue's underlying store file, attempting to keep
 * ahead of any appenders to the queue.
 * <p>
 * Resources held by this object will be released when the underlying queue is closed.
 * <p>
 * Alternatively, the {@code shutdown()} method can be called to close the supplied queue and release any other resources. Invocation of the {@code
 * execute()} method after {@code shutdown()} has been called will cause an {@code IllegalStateException} to be thrown.
 */
public final class Pretoucher extends AbstractCloseable {
    static final long PRETOUCHER_PREROLL_TIME_DEFAULT_MS = 2_000L;
    private final long PRETOUCHER_PREROLL_TIME_MS = Long.getLong("SingleChronicleQueueExcerpts.pretoucherPrerollTimeMs", PRETOUCHER_PREROLL_TIME_DEFAULT_MS);
    private final SingleChronicleQueue queue;
    private final NewChunkListener chunkListener;
    private final IntConsumer cycleChangedListener;
    private final boolean earlyAcquireNextCycle;
    private final boolean canWrite;
    private final PretoucherState pretoucherState;
    private final TimeProvider pretouchTimeProvider;
    private int currentCycle = Integer.MIN_VALUE;
    private SingleChronicleQueueStore currentCycleWireStore;
    private MappedBytes currentCycleMappedBytes;

    public Pretoucher(final SingleChronicleQueue queue) {
        this(queue,
                null,
                c -> {
                },
                Jvm.getBoolean("SingleChronicleQueueExcerpts.earlyAcquireNextCycle", false),
                !Jvm.getBoolean("SingleChronicleQueueExcerpts.dontWrite"));
    }

    // visible for testing
    public Pretoucher(final SingleChronicleQueue queue, final NewChunkListener chunkListener,
                      final IntConsumer cycleChangedListener,
                      boolean earlyAcquireNextCycle,
                      boolean canWrite) {
        this.queue = queue;
        this.chunkListener = chunkListener;
        this.cycleChangedListener = cycleChangedListener;
        this.earlyAcquireNextCycle = checkEA(earlyAcquireNextCycle);
        this.canWrite = canWrite || this.earlyAcquireNextCycle;
        pretoucherState = new PretoucherState(this::getStoreWritePosition);
        if (PRETOUCHER_PREROLL_TIME_MS != PRETOUCHER_PREROLL_TIME_DEFAULT_MS && !earlyAcquireNextCycle)
            Jvm.warn().on(getClass(), "SingleChronicleQueueExcerpts.pretoucherPrerollTimeMs has been set but not earlyAcquireNextCycle");
        pretouchTimeProvider = () -> queue.time().currentTimeMillis() + (this.earlyAcquireNextCycle ? PRETOUCHER_PREROLL_TIME_MS : 0);

        // always put references to "this" last.
        queue.addCloseListener(this);
    }

    private boolean checkEA(boolean ea) {
        if (ea)
            Jvm.warn().on(getClass(), "SingleChronicleQueueExcerpts.earlyAcquireNextCycle is not supported");
        return false;
    }

    public void execute() throws InvalidEventHandlerException {
        try {
            throwExceptionIfClosed();
        } catch (ClosedIllegalStateException e) {
            throw new InvalidEventHandlerException(e);
        }

        try {
            assignCurrentCycle();

            if (currentCycleMappedBytes != null)
                pretoucherState.pretouch(currentCycleMappedBytes);

        } catch (ClassCastException cce) {
            Jvm.warn().on(getClass(), cce);

        } catch (IllegalStateException e) {
            if (queue.isClosed())
                throw new InvalidEventHandlerException(e);
            else
                Jvm.warn().on(getClass(), e);
        }
    }

    public void shutdown() {
        throwExceptionIfClosed();
        queue.close();
    }

    /**
     * used by the pretoucher to acquire the next cycle file, but does NOT do the roll. If configured, we acquire the cycle file early
     */
    private void assignCurrentCycle() {
        int qCycle;

        if (currentCycle < 0 || !canWrite) {
            qCycle = queue.cycle(); // strictly follow current cycle when not creating new cycles
        } else {
            // avoid gaps and not running further than one cycle in the future when creating new cycles
            qCycle = Math.min(currentCycle + 1, Math.min(queue.cycle() + 1, queue.cycle(pretouchTimeProvider)));
        }

        if (qCycle != currentCycle) {
            releaseResources();

            if (canWrite)
                queue.writeLock().lock();
            try {
                currentCycleWireStore = queue.storeForCycle(qCycle, queue.epoch(), canWrite, null);
            } finally {
                if (canWrite)
                    queue.writeLock().unlock();
            }

            if (currentCycleWireStore != null) {
                currentCycleMappedBytes = currentCycleWireStore.bytes();
                currentCycle = qCycle;
                if (chunkListener != null)
                    currentCycleMappedBytes.mappedFile().setNewChunkListener(chunkListener);

                cycleChangedListener.accept(qCycle);

                if (qCycle > queue.cycle()) {
                    if (Jvm.isPerfEnabled(getClass()))
                        Jvm.perf().on(getClass(), "Pretoucher ROLLING early to next file=" + currentCycleWireStore.file());
                }
            }
        }
    }

    private long getStoreWritePosition() {
        return currentCycleWireStore.writePosition();
    }

    private void releaseResources() {
        if (currentCycleWireStore != null) {
            queue.closeStore(currentCycleWireStore);
            currentCycleWireStore = null;
        }
        if (currentCycleMappedBytes != null) {
            currentCycleMappedBytes.close();
            currentCycleMappedBytes = null;
        }
    }

    @Override
    protected void performClose() {
        releaseResources();
    }
}