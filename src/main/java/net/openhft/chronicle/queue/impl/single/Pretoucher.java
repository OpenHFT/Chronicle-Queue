package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.NewChunkListener;
import net.openhft.chronicle.queue.impl.WireStore;

import java.util.function.IntConsumer;

/**
 * A class designed to be called from a long-lived thread.
 * <p>
 * Upon invocation of the {@code execute()} method, this object will pre-touch pages in the supplied queue's underlying store file,
 * attempting to keep ahead of any appenders to the queue.
 * <p>
 * Resources held by this object will be released when the underlying queue is closed.
 * <p>
 * Alternatively, the {@code shutdown()} method can be called to close the supplied queue and release any other resources.
 * Invocation of the {@code execute()} method after {@code shutdown()} has been called with cause an {@code IllegalStateException} to be thrown.
 */
public final class Pretoucher {
    private final SingleChronicleQueue queue;
    private final NewChunkListener chunkListener;
    private final IntConsumer cycleChangedListener;
    private final PretoucherState pretoucherState;
    private int currentCycle = Integer.MIN_VALUE;
    private WireStore currentCycleWireStore;
    private MappedBytes currentCycleMappedBytes;

    public Pretoucher(final SingleChronicleQueue queue) {
        this(queue, null, c -> {
        });
    }

    // visible for testing
    Pretoucher(final SingleChronicleQueue queue, final NewChunkListener chunkListener,
               final IntConsumer cycleChangedListener) {
        this.queue = queue;
        this.chunkListener = chunkListener;
        this.cycleChangedListener = cycleChangedListener;
        queue.addCloseListener(this, Pretoucher::releaseResources);
        pretoucherState = new PretoucherState(this::getStoreWritePosition);
    }

    public void execute() {
        assignCurrentCycle();
        pretoucherState.pretouch(currentCycleMappedBytes);
    }

    public void shutdown() {
        queue.close();
    }

    private void assignCurrentCycle() {
        if (queue.cycle() != currentCycle) {
            releaseResources();

            currentCycleWireStore = queue.storeForCycle(queue.cycle(), queue.epoch(), true);
            currentCycleMappedBytes = currentCycleWireStore.bytes();
            currentCycle = queue.cycle();
            if (chunkListener != null)
                currentCycleMappedBytes.setNewChunkListener(chunkListener);

            cycleChangedListener.accept(queue.cycle());
        }
    }

    private long getStoreWritePosition() {
        return currentCycleWireStore.writePosition();
    }

    private void releaseResources() {
        if (currentCycleWireStore != null) {
            queue.release(currentCycleWireStore);
        }
        if (currentCycleMappedBytes != null) {
            currentCycleMappedBytes.close();
        }
    }
}