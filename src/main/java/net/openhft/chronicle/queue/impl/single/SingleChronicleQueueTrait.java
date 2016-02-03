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
 */
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
public interface SingleChronicleQueueTrait {
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
