/*
 * Copyright 2016-2022 chronicle.software
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

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.NewChunkListener;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.ClosedIllegalStateException;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.wire.Wire;

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
// TODO Move to CQE in x.24
public final class Pretoucher extends AbstractCloseable {
    static final long PRETOUCHER_PREROLL_TIME_DEFAULT_MS = 2_000L;
    private final long PRETOUCHER_PREROLL_TIME_MS = Jvm.getLong("SingleChronicleQueueExcerpts.pretoucherPrerollTimeMs", PRETOUCHER_PREROLL_TIME_DEFAULT_MS);
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
        this.canWrite = canWrite;
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
        final int qCycle = queue.cycle(pretouchTimeProvider);
        if (qCycle != currentCycle) {
            releaseResources();

            if (canWrite)
                queue.writeLock().lock();
            try {
                if (!earlyAcquireNextCycle && currentCycleWireStore != null && canWrite)
                    try {
                        final Wire wire = queue.wireType().apply(currentCycleMappedBytes);
                        wire.usePadding(currentCycleWireStore.dataVersion() > 0);
                        currentCycleWireStore.writeEOF(wire, queue.timeoutMS);
                    } catch (Exception ex) {
                        Jvm.warn().on(getClass(), "unable to write the EOF file=" + currentCycleMappedBytes.mappedFile().file(), ex);
                    }
                SingleChronicleQueueStore oldStore = currentCycleWireStore;
                currentCycleWireStore = queue.storeForCycle(qCycle, queue.epoch(), earlyAcquireNextCycle || canWrite, currentCycleWireStore);
                if (oldStore != null && oldStore != currentCycleWireStore)
                    oldStore.close();
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

                if (earlyAcquireNextCycle)
                    if (Jvm.isDebugEnabled(getClass()))
                        Jvm.debug().on(getClass(), "Pretoucher ROLLING early to next file=" + currentCycleWireStore.file());
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