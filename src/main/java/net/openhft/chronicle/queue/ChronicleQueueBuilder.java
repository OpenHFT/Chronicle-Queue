/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.BytesRingBufferStats;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.queue.impl.WireStore;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * @author Rob Austin.
 */
public interface ChronicleQueueBuilder<B extends ChronicleQueueBuilder, Q extends ChronicleQueue> extends Cloneable {
    @NotNull
    Q build();

    @NotNull
    B onRingBufferStats(@NotNull Consumer<BytesRingBufferStats> onRingBufferStats);

    Consumer<BytesRingBufferStats> onRingBufferStats();

    @NotNull
    File path();

    @NotNull
    B blockSize(int blockSize);

    long blockSize();

    @NotNull
    B wireType(@NotNull WireType wireType);

    @NotNull
    WireType wireType();

    @NotNull
    B rollCycle(@NotNull RollCycle rollCycle);

    long bufferCapacity();

    @NotNull
    B bufferCapacity(long ringBufferSize);

    @NotNull
    B epoch(long epoch);

    long epoch();

    @NotNull
    RollCycle rollCycle();

    @NotNull
    B onThrowable(@NotNull Consumer<Throwable> onThrowable);

    @NotNull
    B buffered(boolean isBuffered);

    boolean buffered();

    @Nullable
    EventLoop eventLoop();

    @NotNull
    B eventLoop(EventLoop eventLoop);

    @NotNull
    B bufferCapacity(int bufferCapacity);

    B indexCount(int indexCount);

    int indexCount();

    B indexSpacing(int indexSpacing);

    int indexSpacing();

    BiFunction<RollingChronicleQueue, Wire, WireStore> storeFactory();
}
