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
import java.util.function.Consumer;

/**
 * @author Rob Austin.
 */
public interface ChronicleQueueBuilder<B extends ChronicleQueueBuilder> extends Cloneable {

    static SingleChronicleQueueBuilder single(@NotNull String basePath) {
        return SingleChronicleQueueBuilder.binary(basePath);
    }

    static SingleChronicleQueueBuilder single(@NotNull File basePath) {
        return SingleChronicleQueueBuilder.binary(basePath);
    }

    @Deprecated
    static SingleChronicleQueueBuilder singleText(@NotNull String basePath) {
        return SingleChronicleQueueBuilder.text(new File(basePath));
    }

    @NotNull
    ChronicleQueue build();

    @NotNull
    B onRingBufferStats(@NotNull Consumer<BytesRingBufferStats> onRingBufferStats);

    @NotNull
    Consumer<BytesRingBufferStats> onRingBufferStats();

    @NotNull
    File path();

    @NotNull
    B blockSize(int blockSize);

    /**
     * THIS IS FOR TESTING ONLY.
     * This makes the block size small to speed up short tests and show up issues which occur when moving from one block to another.
     * <p>
     * Using this will be slower when you have many messages, and break when you have large messages.
     * </p>
     *
     * @return this
     */
    @NotNull
    default B testBlockSize() {
        // small size for testing purposes only.
        return blockSize(256 << 10);
    }

    long blockSize();

    @NotNull
    B wireType(@NotNull WireType wireType);

    @NotNull
    WireType wireType();

    @NotNull
    B rollCycle(@NotNull RollCycle rollCycle);

    @NotNull
    B epoch(long epoch);

    long epoch();

    @NotNull
    RollCycle rollCycle();

    /**
     * @deprecated Use writeBufferMode
     */
    @NotNull
    @Deprecated
    B buffered(boolean isBuffered);

    /**
     * @deprecated Use writeBufferMode
     */
    @Deprecated
    boolean buffered();

    /**
     * @param bufferCapacity to use when buffering enabled.
     * @return this
     */
    @NotNull
    B bufferCapacity(long bufferCapacity);

    long bufferCapacity();

    /**
     * @param writeBufferMode to use for writes. Only None is available in OSS
     * @return this
     */
    B writeBufferMode(BufferMode writeBufferMode);

    @NotNull
    BufferMode writeBufferMode();

    /**
     * @param readBufferMode to use for read. Only None is available in OSS
     * @return this
     */
    B readBufferMode(BufferMode readBufferMode);

    BufferMode readBufferMode();

    /**
     * @param eventLoop to use when asynchronous buffering is used.
     * @return this
     */
    @NotNull
    B eventLoop(EventLoop eventLoop);

    @Nullable
    EventLoop eventLoop();

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
