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
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.BufferMode;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.*;
import net.openhft.chronicle.queue.impl.table.ReadonlyTableStore;
import net.openhft.chronicle.queue.impl.table.SingleTableBuilder;
import net.openhft.chronicle.threads.TimingPauser;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static net.openhft.chronicle.core.pool.ClassAliasPool.CLASS_ALIASES;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueue.QUEUE_METADATA_FILE;
import static net.openhft.chronicle.wire.WireType.DEFAULT_ZERO_BINARY;
import static net.openhft.chronicle.wire.WireType.DELTA_BINARY;

public class SingleChronicleQueueBuilder<S extends SingleChronicleQueueBuilder>
        extends AbstractChronicleQueueBuilder<SingleChronicleQueueBuilder<S>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SingleChronicleQueueBuilder.class);
    private WireStoreFactory storeFactory;
    static {
        CLASS_ALIASES.addAlias(WireType.class);
        CLASS_ALIASES.addAlias(SCQMeta.class, "SCQMeta");
        CLASS_ALIASES.addAlias(SCQRoll.class, "SCQSRoll");
        CLASS_ALIASES.addAlias(SCQIndexing.class, "SCQSIndexing");
        CLASS_ALIASES.addAlias(SingleChronicleQueueStore.class, "SCQStore");
        CLASS_ALIASES.addAlias(TimedStoreRecovery.class);
    }

    protected TableStore<SCQMeta> metaStore;

    public SingleChronicleQueueBuilder() {
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
    }

    @Override
    public WireStoreFactory storeFactory() {
        return storeFactory == null ? SingleChronicleQueueBuilder::createStore : storeFactory;
    }

    public static void addAliases() {
        // static initialiser.
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
        if (file.isFile()) {
            if (!file.getName().endsWith(SingleChronicleQueue.SUFFIX)) {
                throw new IllegalArgumentException("Invalid file type: " + file.getName());
            }

            LOGGER.warn("Queues should be configured with the queue directory, not a specific filename. Actual file used: {}",
                    file.getParentFile());

            return new SingleChronicleQueueBuilder<>(file.getParentFile())
                    .wireType(wireType);
        }
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

    @NotNull
    public static SingleChronicleQueueBuilder deltaBinary(@NotNull File basePathFile) {
        return builder(basePathFile, DELTA_BINARY);
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
    static SingleChronicleQueueStore createStore(@NotNull RollingChronicleQueue queue,
                                                 @NotNull Wire wire) {
        final SingleChronicleQueueStore wireStore = new SingleChronicleQueueStore(
                queue.rollCycle(),
                queue.wireType(),
                (MappedBytes) wire.bytes(),
                queue.indexCount(),
                queue.indexSpacing());

        wire.writeEventName(MetaDataKeys.header).typedMarshallable(wireStore);

        return wireStore;
    }

    @Nullable
    static SingleChronicleQueueStore loadStore(@NotNull Wire wire) {
        final StringBuilder eventName = new StringBuilder();
        wire.readEventName(eventName);
        if (eventName.toString().equals(MetaDataKeys.header.name())) {
            final SingleChronicleQueueStore store = wire.read().typedMarshallable();
            if (store == null) {
                throw new IllegalArgumentException("Unable to load wire store");
            }
            return store;
        }

        LOGGER.warn("Unable to load store file from input. Queue file may be corrupted.");
        return null;
    }

    // *************************************************************************
    //
    // *************************************************************************

    private static boolean isQueueReplicationAvailable() {
        try {
            Class.forName("software.chronicle.enterprise.queue.QueueSyncReplicationHandler");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    @Override
    @NotNull
    public SingleChronicleQueue build() {
        if (readBufferMode() != BufferMode.None)
            onlyAvailableInEnterprise("Buffering");
        if (writeBufferMode() != BufferMode.None)
            onlyAvailableInEnterprise("Buffering");
        super.preBuild();
        return new SingleChronicleQueue(this);
    }

    private void onlyAvailableInEnterprise(final String feature) {
        getLogger().warn(feature + " is only supported in Chronicle Queue Enterprise. " +
                "If you would like to use this feature, please contact sales@chronicle.software for more information.");
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
    public SingleChronicleQueueBuilder<S> blockSize(long blockSize) {
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
    public SingleChronicleQueueBuilder<S> pauserSupplier(Supplier<TimingPauser> pauser) {
        return super.pauserSupplier(pauser);
    }

    @Override
    public SingleChronicleQueueBuilder<S> path(final String path) {
        return super.path(path);
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
    public SingleChronicleQueueBuilder<S> rollTime(@NotNull final LocalTime time, final ZoneId zoneId) {
        if (!zoneId.equals(ZoneId.of("UTC"))) {
            onlyAvailableInEnterprise("Non-UTC time-zone");
        }
        return super.rollTime(time, ZoneId.of("UTC"));
    }

    @Override
    protected void initializeMetadata() {
        File metapath = metapath();
        validateRollCycle(metapath);
        SCQMeta metadata = new SCQMeta(new SCQRoll(rollCycle(), epoch()), deltaCheckpointInterval(),
                sourceId());
        try {

            boolean readOnly = readOnly();
            metaStore = SingleTableBuilder.binary(metapath, metadata).timeoutMS(timeoutMS())
                    .readOnly(readOnly).validateMetadata(!readOnly).build();
            // check if metadata was overridden
            SCQMeta newMeta = metaStore.metadata();
            if (sourceId() == 0)
                sourceId(newMeta.sourceId());

            if (readOnly && !newMeta.roll().format().equals(rollCycle().format())) {
                // roll cycle changed
                overrideRollCycleForFileNameLength(newMeta.roll().format().length());
            }
        } catch (IORuntimeException ex) {
            // readonly=true and file doesn't exist
            metaStore = new ReadonlyTableStore<>(metadata);
        }
    }

    private void validateRollCycle(File metapath) {
        if (!metapath.exists()) {
            // no metadata, so we need to check if there're cq4 files and if so try to validate roll cycle
            // the code is slightly brutal and crude but should work for most cases. It will NOT work if files were created with
            // the following cycles: LARGE_HOURLY_SPARSE LARGE_HOURLY_XSPARSE LARGE_DAILY XLARGE_DAILY HUGE_DAILY HUGE_DAILY_XSPARSE
            // for such cases user MUST use correct roll cycle when creating the queue
            String[] list = path.list((d, name) -> name.endsWith(SingleChronicleQueue.SUFFIX));
            if (list != null && list.length > 0) {
                String filename = list[0];
                if (rollCycle().format().length() + 4 != filename.length()) {
                    // probably different roll cycle used
                    overrideRollCycleForFileNameLength(filename.length() - 4);
                }
            }
        }
    }

    private void overrideRollCycleForFileNameLength(int patternLength) {
        for (RollCycles cycle : RollCycles.values()) {
            if (cycle.format().length() == patternLength) {
                LOGGER.warn("Overriding roll cycle to " + cycle);
                rollCycle = cycle;
                break;
            }
        }
    }

    private File metapath() {
        final File storeFilePath;
        if ("".equals(path.getPath())) {
            storeFilePath = new File(QUEUE_METADATA_FILE);
        } else {
            storeFilePath = new File(path, QUEUE_METADATA_FILE);
            path.mkdirs();
        }
        return storeFilePath;
    }

    @NotNull
    protected QueueLock queueLock() {
        return isQueueReplicationAvailable() && !readOnly() ? new TSQueueLock(metaStore, pauserSupplier(), timeoutMS() * 3 / 2) : new NoopQueueLock();
    }

    @NotNull
    protected WriteLock writeLock() {
        return readOnly() ? new ReadOnlyWriteLock() : new TableStoreWriteLock(metaStore, pauserSupplier(), timeoutMS() * 3 / 2);
    }

    protected int deltaCheckpointInterval() {
        return -1;
    }

    protected TableStore<SCQMeta> metaStore() {
        return metaStore;
    }
}