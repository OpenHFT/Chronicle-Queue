/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.BytesRingBufferStats;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.queue.impl.WireStoreFactory;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.util.TimeZone;
import java.util.function.Consumer;

/**
 * @author Rob Austin.
 */
public interface ChronicleQueueBuilder<B extends ChronicleQueueBuilder<B, Q>, Q extends ChronicleQueue> extends Cloneable {

    static SingleChronicleQueueBuilder single(String basePath) {
        return SingleChronicleQueueBuilder.binary(basePath);
    }

    @Deprecated
    static SingleChronicleQueueBuilder singleText(String basePath) {
        return SingleChronicleQueueBuilder.text(new File(basePath));
    }

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

    WireStoreFactory storeFactory();

    B storeFileListener(StoreFileListener storeFileListener);

    StoreFileListener storeFileListener();

    boolean readOnly();

    B readOnly(boolean readOnly);
}
