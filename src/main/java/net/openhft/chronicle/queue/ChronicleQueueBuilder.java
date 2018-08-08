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
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.threads.TimingPauser;
import net.openhft.chronicle.wire.FieldInfo;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.builder;

/**
 * @author Rob Austin.
 */
public interface ChronicleQueueBuilder<B extends ChronicleQueueBuilder, Q extends ChronicleQueue>
        extends Cloneable, Marshallable {

    static SingleChronicleQueueBuilder<SingleChronicleQueueBuilder, SingleChronicleQueue> single() {
        SingleChronicleQueueBuilder<SingleChronicleQueueBuilder, SingleChronicleQueue> builder = builder();
        builder.wireType(WireType.BINARY_LIGHT);
        return builder;
    }

    static SingleChronicleQueueBuilder<SingleChronicleQueueBuilder, SingleChronicleQueue> single(@NotNull String basePath) {
        return SingleChronicleQueueBuilder.binary(basePath);
    }

    static SingleChronicleQueueBuilder<SingleChronicleQueueBuilder, SingleChronicleQueue> single(@NotNull File basePath) {
        return SingleChronicleQueueBuilder.binary(basePath);
    }

    @Deprecated
    static SingleChronicleQueueBuilder<SingleChronicleQueueBuilder, SingleChronicleQueue> singleText(@NotNull String basePath) {
        return SingleChronicleQueueBuilder.text(new File(basePath));
    }

    @NotNull
    Q build();

    @NotNull
    B onRingBufferStats(@NotNull Consumer<BytesRingBufferStats> onRingBufferStats);

    @NotNull
    Consumer<BytesRingBufferStats> onRingBufferStats();

    @NotNull
    File path();

    /**
     * you should make sure that your largest message size is no larger than 1/4 of your block size
     *
     * @param blockSize the size of the off heap memory mapping
     * @return that
     */
    @NotNull
    B blockSize(int blockSize);

    B blockSize(long blockSize);

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
        return blockSize(64 << 10);
    }

    long blockSize();

    @NotNull
    B wireType(@NotNull WireType wireType);

    @NotNull
    WireType wireType();

    @NotNull
    B rollCycle(@NotNull RollCycle rollCycle);

    /**
     * Resets the concept of 'epoch' from 1970-01-01T00:00:00 UTC to a new value (in UTC millis).
     * <p>
     * This method is deprecated and will be removed in a future release.
     * <p>
     * Please use the <code>rollTime</code> method, specifying the new epoch as a
     * <code>LocalTime</code> that will be resolved against UTC.
     *
     * @param epoch a value in UTC millis that will be used when resolving roll cycle times.
     * @return the builder
     */
    @NotNull
    @Deprecated
    B epoch(long epoch);

    long epoch();

    /**
     * Resets the rollTime for the queue cycle to a new time.
     * <p>
     * E.g. builder.rollTime(LocalTime.of(21, 0), ZoneId.of("UTC"))
     * will cause the queue to roll cycles at 21:00 UTC,
     * rather than the default roll-time of midnight UTC.
     *
     * @param time   the new value for the time of day when the cycle should roll
     * @param zoneId the time-zone against which to base the roll-time
     * @return the builder
     */
    B rollTime(@NotNull LocalTime time, final ZoneId zoneId);

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

    boolean hasPretouchIntervalMillis();

    boolean readOnly();

    B readOnly(boolean readOnly);

    CycleCalculator cycleCalculator();

    /**
     * Hold a strong reference to any appender instead of a weak one which prevents GC if the thread and queue exists.
     *
     * @param strongAppenders use strong references if true, use weak references is false.
     * @return this builder.
     */
    B strongAppenders(boolean strongAppenders);

    boolean strongAppenders();

    Object clone();

    B pauserSupplier(Supplier<TimingPauser> timedBusy);

    default B path(String path) {
        return path(new File(path));
    }

    B path(File path);

    /**
     * updates all the fields in {@code this} that are null, from the {@param parentBuilder}
     *
     * @param parentBuilder the parentBuilder Chronicle Queue Builder
     * @return that
     */

    default B setAllNullFields(@Nullable ChronicleQueueBuilder parentBuilder) {
        if (parentBuilder == null)
            return (B) this;

        List<FieldInfo> sourceFieldInfo = Wires.fieldInfos(parentBuilder.getClass());

        for (final FieldInfo fieldInfo : Wires.fieldInfos(this.getClass())) {
            if (!sourceFieldInfo.contains(fieldInfo))
                continue;
            Object resultV = fieldInfo.get(this);
            Object parentV = fieldInfo.get(parentBuilder);
            if (resultV == null && parentV != null)
                fieldInfo.set(this, parentV);

        }
        return (B) this;
    }

    /**
     * @return true if the bockSize has been set, Hence is non null
     */
    boolean hasBlockSize();

}
