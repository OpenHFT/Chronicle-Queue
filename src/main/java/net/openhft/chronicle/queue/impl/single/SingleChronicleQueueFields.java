package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.BytesRingBufferStats;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.threads.api.EventLoop;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.util.function.Consumer;

/**
 * @author Rob Austin.
 */
public interface SingleChronicleQueueFields {
    Consumer<BytesRingBufferStats> onRingBufferStats();

    @NotNull
    File path();

    long blockSize();

    @NotNull
    WireType wireType();

    long bufferCapacity();

    long epoch();

    @NotNull
    RollCycle rollCycle();

    boolean buffered();

    @Nullable
    EventLoop eventLoop();
}
