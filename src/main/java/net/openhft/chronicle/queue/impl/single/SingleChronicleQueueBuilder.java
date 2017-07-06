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
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.BufferMode;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.impl.AbstractChronicleQueueBuilder;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.nio.file.Path;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static net.openhft.chronicle.core.pool.ClassAliasPool.CLASS_ALIASES;
import static net.openhft.chronicle.wire.WireType.DEFAULT_ZERO_BINARY;

public class SingleChronicleQueueBuilder<S extends SingleChronicleQueueBuilder>
        extends AbstractChronicleQueueBuilder<SingleChronicleQueueBuilder<S>> {
    static {
        CLASS_ALIASES.addAlias(WireType.class);
        CLASS_ALIASES.addAlias(SCQRoll.class, "SCQSRoll");
        CLASS_ALIASES.addAlias(SCQIndexing.class, "SCQSIndexing");
        CLASS_ALIASES.addAlias(SingleChronicleQueueStore.class, "SCQStore");
        CLASS_ALIASES.addAlias(TimedStoreRecovery.class);
    }

    @SuppressWarnings("unchecked")
    @Deprecated
    public SingleChronicleQueueBuilder(@NotNull String path) {
        this(new File(path));
    }

    @SuppressWarnings("unchecked")
    @Deprecated
    public SingleChronicleQueueBuilder(@NotNull File path) {
        super(path);
        storeFactory(SingleChronicleQueueBuilder::createStore);
    }

    public static void init() {
        // make sure the static block has been called.
    }

    @NotNull
    public static <S extends SingleChronicleQueueBuilder<S>> SingleChronicleQueueBuilder<S> builder(@NotNull Path path, @NotNull WireType wireType) {
        return builder(path.toFile(), wireType);
    }

    @NotNull
    public static SingleChronicleQueueBuilder builder(@NotNull File file, @NotNull WireType wireType) {
        return new SingleChronicleQueueBuilder<>(file)
                .wireType(wireType);
    }

    @NotNull
    public static SingleChronicleQueueBuilder binary(@NotNull Path path) {
        return binary(path.toFile());
    }

    @NotNull
    public static SingleChronicleQueueBuilder binary(@NotNull String basePath) {
        return binary(new File(basePath));
    }

    @NotNull
    public static SingleChronicleQueueBuilder binary(@NotNull File basePathFile) {
        return builder(basePathFile, WireType.BINARY_LIGHT);
    }

    @NotNull
    public static SingleChronicleQueueBuilder fieldlessBinary(@NotNull File name) {
        return builder(name, WireType.FIELDLESS_BINARY);
    }

    @NotNull
    public static SingleChronicleQueueBuilder defaultZeroBinary(@NotNull File basePathFile) {
        return builder(basePathFile, DEFAULT_ZERO_BINARY);
    }

    @Deprecated
    @NotNull
    public static SingleChronicleQueueBuilder text(@NotNull File name) {
        return builder(name, WireType.TEXT);
    }

    // *************************************************************************
    //
    // *************************************************************************

    @NotNull
    static SingleChronicleQueueStore createStore(@NotNull RollingChronicleQueue queue, @NotNull Wire wire) {
        final SingleChronicleQueueStore wireStore = new SingleChronicleQueueStore(
                queue.rollCycle(),
                queue.wireType(),
                (MappedBytes) wire.bytes(),
                queue.epoch(),
                queue.indexCount(),
                queue.indexSpacing(),
                queue.recoverySupplier().apply(queue.wireType()),
                queue.deltaCheckpointInterval());

        wire.writeEventName(MetaDataKeys.header).typedMarshallable(wireStore);

        return wireStore;
    }

    // *************************************************************************
    //
    // *************************************************************************

    @NotNull
    public SingleChronicleQueue build() {
        if (buffered())
            onlyAvailableInEnterprise("Buffering");
        return new SingleChronicleQueue(this);
    }

    private void onlyAvailableInEnterprise(final String feature) {
        getLogger().warn(feature + " is only supported in Chronicle Queue Enterprise. " +
                "If you would like to use this feature, please contact sales@chronicle.software for more information.");
    }

    @NotNull
    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    @Override
    public SingleChronicleQueueBuilder<S> clone() {
        try {
            @SuppressWarnings("unchecked")
            SingleChronicleQueueBuilder<S> clone = (SingleChronicleQueueBuilder<S>) super.clone();
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

    @Nullable
    public Supplier<BiConsumer<BytesStore, Bytes>> encodingSupplier() {
        return null;
    }

    @Nullable
    public Supplier<BiConsumer<BytesStore, Bytes>> decodingSupplier() {
        return null;
    }

    @NotNull
    public SingleChronicleQueueBuilder aesEncryption(@Nullable byte[] keyBytes) {
        if (keyBytes == null) {
            codingSuppliers(null, null);
            return this;
        }
        onlyAvailableInEnterprise("AES encryption");
        return this;
    }

    @NotNull
    public SingleChronicleQueueBuilder codingSuppliers(@Nullable Supplier<BiConsumer<BytesStore, Bytes>> encodingSupplier,
                                                       @Nullable Supplier<BiConsumer<BytesStore, Bytes>> decodingSupplier) {
        if (encodingSupplier != null || decodingSupplier != null)
            onlyAvailableInEnterprise("Custom encoding");
        return this;
    }

    @NotNull
    @Override
    public SingleChronicleQueueBuilder<S> testBlockSize() {
        super.testBlockSize();
        return this;
    }

    @Override
    public SingleChronicleQueueBuilder<S> sourceId(int sourceId) {
        return super.sourceId(sourceId);
    }

    @NotNull
    @Override
    public SingleChronicleQueueBuilder<S> blockSize(int blockSize) {
        return super.blockSize(blockSize);
    }

    @NotNull
    @Override
    public SingleChronicleQueueBuilder<S> wireType(@NotNull WireType wireType) {
        return super.wireType(wireType);
    }

    @NotNull
    @Override
    public SingleChronicleQueueBuilder<S> rollCycle(@NotNull RollCycle rollCycle) {
        return super.rollCycle(rollCycle);
    }

    @NotNull
    @Override
    public SingleChronicleQueueBuilder<S> bufferCapacity(long bufferCapacity) {
        return super.bufferCapacity(bufferCapacity);
    }

    @NotNull
    @Override
    public SingleChronicleQueueBuilder<S> epoch(long epoch) {
        return super.epoch(epoch);
    }

    @NotNull
    @Override
    public SingleChronicleQueueBuilder<S> buffered(boolean isBuffered) {
        return super.buffered(isBuffered);
    }

    @Override
    public SingleChronicleQueueBuilder<S> writeBufferMode(BufferMode writeBufferMode) {
        return super.writeBufferMode(writeBufferMode);
    }

    @Override
    public SingleChronicleQueueBuilder<S> readBufferMode(BufferMode readBufferMode) {
        return super.readBufferMode(readBufferMode);
    }

    @NotNull
    @Override
    public SingleChronicleQueueBuilder<S> eventLoop(EventLoop eventLoop) {
        return super.eventLoop(eventLoop);
    }

    @Override
    public SingleChronicleQueueBuilder<S> indexCount(int indexCount) {
        return super.indexCount(indexCount);
    }

    @Override
    public SingleChronicleQueueBuilder<S> indexSpacing(int indexSpacing) {
        return super.indexSpacing(indexSpacing);
    }

    @Override
    public SingleChronicleQueueBuilder<S> timeProvider(TimeProvider timeProvider) {
        return super.timeProvider(timeProvider);
    }

    @Override
    public SingleChronicleQueueBuilder<S> pauserSupplier(Supplier<Pauser> pauser) {
        return super.pauserSupplier(pauser);
    }

    @Override
    public SingleChronicleQueueBuilder<S> timeoutMS(long timeoutMS) {
        return super.timeoutMS(timeoutMS);
    }

    @Override
    public SingleChronicleQueueBuilder<S> readOnly(boolean readOnly) {
        return super.readOnly(readOnly);
    }

    @Override
    public SingleChronicleQueueBuilder<S> storeFileListener(StoreFileListener storeFileListener) {
        return super.storeFileListener(storeFileListener);
    }

    @Override
    public SingleChronicleQueueBuilder<S> recoverySupplier(StoreRecoveryFactory recoverySupplier) {
        return super.recoverySupplier(recoverySupplier);
    }

    @Override
    public SingleChronicleQueueBuilder<S> rollTime(@NotNull final ZonedDateTime time) {
        if (!time.getOffset().equals(ZoneOffset.UTC)) {
            onlyAvailableInEnterprise("Non-UTC time-zone");
        }
        return super.rollTime(time.withZoneSameLocal(ZoneId.of("UTC")));
    }
}