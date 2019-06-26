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
import net.openhft.chronicle.bytes.BytesRingBufferStats;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.core.util.ObjectUtils;
import net.openhft.chronicle.core.util.ThrowingBiFunction;
import net.openhft.chronicle.core.util.Updater;
import net.openhft.chronicle.queue.BufferMode;
import net.openhft.chronicle.queue.QueueOffsetSpec;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.queue.impl.WireStoreFactory;
import net.openhft.chronicle.queue.impl.table.ReadonlyTableStore;
import net.openhft.chronicle.queue.impl.table.SingleTableBuilder;
import net.openhft.chronicle.threads.EventGroup;
import net.openhft.chronicle.threads.TimeoutPauser;
import net.openhft.chronicle.threads.TimingPauser;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.lang.reflect.Constructor;
import java.nio.file.Path;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static net.openhft.chronicle.core.pool.ClassAliasPool.CLASS_ALIASES;
import static net.openhft.chronicle.queue.ChronicleQueue.TEST_BLOCK_SIZE;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueue.QUEUE_METADATA_FILE;
import static net.openhft.chronicle.wire.WireType.DEFAULT_ZERO_BINARY;
import static net.openhft.chronicle.wire.WireType.DELTA_BINARY;

public class SingleChronicleQueueBuilder implements Cloneable, Marshallable {
    public static final String DEFAULT_ROLL_CYCLE_PROPERTY = "net.openhft.queue.builder.defaultRollCycle";
    private static final Constructor ENTERPISE_QUEUE_CONSTRUCTOR;
    private static final String DEFAULT_EPOCH_PROPERTY = "net.openhft.queue.builder.defaultEpoch";
    private static final Logger LOGGER = LoggerFactory.getLogger(SingleChronicleQueueBuilder.class);

    static {
        CLASS_ALIASES.addAlias(WireType.class);
        CLASS_ALIASES.addAlias(SCQMeta.class, "SCQMeta");
        CLASS_ALIASES.addAlias(SCQRoll.class, "SCQSRoll");
        CLASS_ALIASES.addAlias(SCQIndexing.class, "SCQSIndexing");
        CLASS_ALIASES.addAlias(SingleChronicleQueueStore.class, "SCQStore");

        {
            Constructor co;
            try {
                co = ((Class) Class.forName("software.chronicle.enterprise.queue.EnterpriseSingleChronicleQueue")).getDeclaredConstructors()[0];
                Jvm.setAccessible(co);
            } catch (Exception e) {
                co = null;
            }
            ENTERPISE_QUEUE_CONSTRUCTOR = co;
        }

    }

    public BufferMode writeBufferMode = BufferMode.None;
    public BufferMode readBufferMode = BufferMode.None;
    private WireType wireType = WireType.BINARY_LIGHT;
    private Long blockSize;
    private File path;
    private RollCycle rollCycle = loadDefaultRollCycle();
    private Long epoch; // default is 1970-01-01 00:00:00.000 UTC
    private Long bufferCapacity;
    private Integer indexSpacing;
    private Integer indexCount;
    private Boolean enableRingBufferMonitoring;
    private Boolean ringBufferReaderCanDrain;
    @Nullable
    private EventLoop eventLoop;
    private WireStoreFactory storeFactory = SingleChronicleQueueBuilder::createStore;
    /**
     * by default logs the performance stats of the ring buffer
     */
    @NotNull
    private Consumer<BytesRingBufferStats> onRingBufferStats = NoBytesRingBufferStats.NONE;
    private TimeProvider timeProvider = SystemTimeProvider.INSTANCE;
    private Supplier<TimingPauser> pauserSupplier = () -> new TimeoutPauser(500_000);
    private Long timeoutMS; // 10 seconds.
    private Integer sourceId;
    private StoreFileListener storeFileListener;

    private Boolean readOnly;
    private Boolean strongAppenders;
    private Boolean checkInterrupts;

    private TableStore<SCQMeta> metaStore;

    // enterprise stuff
    private int deltaCheckpointInterval = -1;
    private Supplier<BiConsumer<BytesStore, Bytes>> encodingSupplier;
    private Supplier<BiConsumer<BytesStore, Bytes>> decodingSupplier;
    private Updater<Bytes> messageInitializer;
    private Consumer<Bytes> messageHeaderReader;
    private SecretKeySpec key;

    private int maxTailers;
    private ThrowingBiFunction<Long, Integer, BytesStore, Exception> bufferBytesStoreCreator;
    private Long pretouchIntervalMillis;
    private LocalTime rollTime;
    private ZoneId rollTimeZone;
    private QueueOffsetSpec queueOffsetSpec;

    protected SingleChronicleQueueBuilder() {
    }
    /*
     * ========================
     * Builders
     * ========================
     */

    public static void addAliases() {
        // static initialiser.
    }

    /**
     * @return an empty builder
     */
    public static SingleChronicleQueueBuilder builder() {
        return new SingleChronicleQueueBuilder();
    }

    @NotNull
    public static SingleChronicleQueueBuilder builder(@NotNull Path path, @NotNull WireType wireType) {
        return builder(path.toFile(), wireType);
    }

    @NotNull
    public static SingleChronicleQueueBuilder builder(@NotNull File file, @NotNull WireType wireType) {
        SingleChronicleQueueBuilder result = builder().wireType(wireType);
        if (file.isFile()) {
            if (!file.getName().endsWith(SingleChronicleQueue.SUFFIX)) {
                throw new IllegalArgumentException("Invalid file type: " + file.getName());
            }

            LOGGER.warn("Queues should be configured with the queue directory, not a specific filename. Actual file used: {}",
                    file.getParentFile());

            result.path(file.getParentFile());
        } else
            result.path(file);

        return result;
    }

    public static SingleChronicleQueueBuilder single() {
        SingleChronicleQueueBuilder builder = builder();
        builder.wireType(WireType.BINARY_LIGHT);
        return builder;
    }

    public static SingleChronicleQueueBuilder single(@NotNull String basePath) {
        return binary(basePath);
    }

    public static SingleChronicleQueueBuilder single(@NotNull File basePath) {
        return binary(basePath);
    }

    public static SingleChronicleQueueBuilder binary(@NotNull Path path) {
        return binary(path.toFile());
    }

    public static SingleChronicleQueueBuilder binary(@NotNull String basePath) {
        return binary(new File(basePath));
    }

    public static SingleChronicleQueueBuilder binary(@NotNull File basePathFile) {
        return builder(basePathFile, WireType.BINARY_LIGHT);
    }

    public static SingleChronicleQueueBuilder fieldlessBinary(@NotNull File name) {
        return builder(name, WireType.FIELDLESS_BINARY);
    }

    public static SingleChronicleQueueBuilder defaultZeroBinary(@NotNull File basePathFile) {
        return builder(basePathFile, DEFAULT_ZERO_BINARY);
    }

    public static SingleChronicleQueueBuilder deltaBinary(@NotNull File basePathFile) {
        return builder(basePathFile, DELTA_BINARY);
    }

    /**
     * @param name               the file name
     * @param deltaIntervalShift default value of 6, the shift for deltaInterval, the should be a
     *                           number between 0-63 ( inclusive ), default the delta messaging is
     *                           check pointed every 64 messages, so the default {@code
     *                           deltaIntervalShift  == 6}, as {@code 1 << 6 == 64 }
     * @return the SingleChronicleQueueBuilder
     */
    public static SingleChronicleQueueBuilder deltaBinary(@NotNull File name, byte deltaIntervalShift) {
        @NotNull SingleChronicleQueueBuilder ret = deltaBinary(name);

        if (deltaIntervalShift < 0 || deltaIntervalShift > 63)
            throw new IllegalArgumentException("deltaIntervalShift=" + deltaIntervalShift + ", but " +
                    "should be a value between 0-63 inclusive");

        ret.deltaCheckpointInterval(1 << deltaIntervalShift);
        return ret;
    }

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

    private static boolean isQueueReplicationAvailable() {
        return ENTERPISE_QUEUE_CONSTRUCTOR != null;
    }

    private static RollCycle loadDefaultRollCycle() {
        if (null == System.getProperty(DEFAULT_ROLL_CYCLE_PROPERTY)) {
            return RollCycles.DAILY;
        }

        String rollCycleProperty = System.getProperty(DEFAULT_ROLL_CYCLE_PROPERTY);
        String[] rollCyclePropertyParts = rollCycleProperty.split(":");
        if (rollCyclePropertyParts.length > 0) {
            try {
                Class rollCycleClass = Class.forName(rollCyclePropertyParts[0]);
                if (Enum.class.isAssignableFrom(rollCycleClass)) {
                    if (rollCyclePropertyParts.length < 2) {
                        LOGGER.warn("Default roll cycle configured as enum, but enum value not specified: " + rollCycleProperty);
                    } else {
                        Class<Enum> eClass = (Class<Enum>) rollCycleClass;
                        Object instance = ObjectUtils.valueOf(eClass, rollCyclePropertyParts[1]);
                        if (instance instanceof RollCycle) {
                            return (RollCycle) instance;
                        } else {
                            LOGGER.warn("Configured default rollcycle is not a subclass of RollCycle");
                        }
                    }
                } else {
                    Object instance = ObjectUtils.newInstance(rollCycleClass);
                    if (instance instanceof RollCycle) {
                        return (RollCycle) instance;
                    } else {
                        LOGGER.warn("Configured default rollcycle is not a subclass of RollCycle");
                    }
                }
            } catch (ClassNotFoundException ignored) {
                LOGGER.warn("Default roll cycle class: " + rollCyclePropertyParts[0] + " was not found");
            }
        }

        return RollCycles.DAILY;
    }

    public WireStoreFactory storeFactory() {
        return storeFactory;
    }

    @NotNull
    public SingleChronicleQueue build() {
        boolean needEnterprise = checkEnterpriseFeaturesRequested();
        preBuild();

        if (needEnterprise)
            return buildEnterprise();

        return new SingleChronicleQueue(this);
    }

    private boolean checkEnterpriseFeaturesRequested() {

        boolean result = false;
        if (readBufferMode != BufferMode.None)
            result = onlyAvailableInEnterprise("Buffering");
        if (writeBufferMode != BufferMode.None)
            result = onlyAvailableInEnterprise("Buffering");
        if (rollTimeZone != null && !rollTimeZone.getId().equals("UTC") && !rollTimeZone.getId().equals("Z"))
            result = onlyAvailableInEnterprise("Non-UTC roll time zone");
        if (wireType == WireType.DELTA_BINARY)
            result = onlyAvailableInEnterprise("Wire type " + wireType.name());
        if (encodingSupplier != null)
            result = onlyAvailableInEnterprise("Encoding");
        if (key != null)
            result = onlyAvailableInEnterprise("Encryption");
        if (hasPretouchIntervalMillis())
            result = onlyAvailableInEnterprise("Out of process pretouching");

        return result;
    }

    private boolean onlyAvailableInEnterprise(final String feature) {
        if (ENTERPISE_QUEUE_CONSTRUCTOR == null)
            LOGGER.warn(feature + " is only supported in Chronicle Queue Enterprise. If you would like to use this feature, please contact sales@chronicle.software for more information.");
        return true;
    }

    @NotNull
    private SingleChronicleQueue buildEnterprise() {
        if (ENTERPISE_QUEUE_CONSTRUCTOR == null)
            throw new IllegalStateException("Enterprise features requested but Chronicle Queue Enterprise is not in the class path!");

        try {
            return (SingleChronicleQueue) ENTERPISE_QUEUE_CONSTRUCTOR.newInstance(this);
        } catch (Exception e) {
            throw new IllegalStateException("Couldn't create an instance of Enterprise queue", e);
        }

    }

    public SingleChronicleQueueBuilder aesEncryption(@Nullable byte[] keyBytes) {
        if (keyBytes == null) {
            codingSuppliers(null, null);
            return this;
        }
        key = new SecretKeySpec(keyBytes, "AES");
        return this;
    }

    public Updater<Bytes> messageInitializer() {
        return messageInitializer == null ? Bytes::clear : messageInitializer;
    }

    public Consumer<Bytes> messageHeaderReader() {
        return messageHeaderReader == null ? b -> {
        } : messageHeaderReader;
    }

    public SingleChronicleQueueBuilder messageHeader(Updater<Bytes> messageInitializer,
                                                     Consumer<Bytes> messageHeaderReader) {
        this.messageInitializer = messageInitializer;
        this.messageHeaderReader = messageHeaderReader;
        return this;
    }

    public SingleChronicleQueueBuilder rollTime(final LocalTime rollTime) {
        rollTime(rollTime, rollTimeZone);
        return this;
    }

    public ZoneId rollTimeZone() {
        return rollTimeZone;
    }

    public SingleChronicleQueueBuilder rollTimeZone(final ZoneId rollTimeZone) {
        rollTime(rollTime, rollTimeZone);
        return this;
    }

    public SingleChronicleQueueBuilder rollTime(@NotNull final LocalTime rollTime, final ZoneId zoneId) {
        this.rollTime = rollTime;
        this.rollTimeZone = zoneId;
        this.epoch = TimeUnit.SECONDS.toMillis(rollTime.toSecondOfDay());
        this.queueOffsetSpec = QueueOffsetSpec.ofRollTime(rollTime, zoneId);
        return this;
    }

    protected void initializeMetadata() {
        File metapath = metapath();
        validateRollCycle(metapath);
        SCQMeta metadata = new SCQMeta(new SCQRoll(rollCycle(), epoch(), rollTime, rollTimeZone), deltaCheckpointInterval(),
                sourceId());
        try {

            boolean readOnly = readOnly();
            metaStore = SingleTableBuilder.binary(metapath, metadata).readOnly(readOnly).build();
            // check if metadata was overridden
            SCQMeta newMeta = metaStore.metadata();
            if (sourceId() == 0)
                sourceId(newMeta.sourceId());

            if (!newMeta.roll().format().equals(rollCycle().format())) {
                // roll cycle changed
                overrideRollCycleForFileNameLength(newMeta.roll().format().length());
            }

            // if it was overriden - reset
            rollTime = newMeta.roll().rollTime();
            rollTimeZone = newMeta.roll().rollTimeZone();
            epoch = newMeta.roll().epoch();
        } catch (IORuntimeException ex) {
            // readonly=true and file doesn't exist
            if (OS.isWindows())
                throw ex; // we cant have a read-only table store on windows so we have no option but to throw the ex.
            Jvm.warn().on(getClass(), "Failback to readonly tablestore", ex);
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
                LOGGER.warn("Overriding roll cycle from {} to {}", rollCycle, cycle);
                rollCycle = cycle;
                return;
            }
        }
        throw new IllegalStateException("Can't find an appropriate RollCycles to override to");
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
    QueueLock queueLock() {
        return isQueueReplicationAvailable() && !readOnly() ? new TSQueueLock(metaStore, pauserSupplier(), timeoutMS() * 3 / 2) : new NoopQueueLock();
    }

    @NotNull
    WriteLock writeLock() {
        return readOnly() ? new ReadOnlyWriteLock() : new TableStoreWriteLock(metaStore, pauserSupplier(), timeoutMS() * 3 / 2);
    }

    public int deltaCheckpointInterval() {
        return deltaCheckpointInterval == -1 ? 64 : deltaCheckpointInterval;
    }

    public QueueOffsetSpec queueOffsetSpec() {
        return queueOffsetSpec == null ? QueueOffsetSpec.ofNone() : queueOffsetSpec;
    }

    TableStore<SCQMeta> metaStore() {
        return metaStore;
    }

    /**
     * RingBuffer tailers need to be preallocated. Only set this if using readBufferMode=Asynchronous.
     * By default 1 tailer will be created for the user.
     *
     * @param maxTailers number of tailers that will be required from this queue, not including the draining tailer
     * @return this
     */
    public SingleChronicleQueueBuilder maxTailers(int maxTailers) {
        this.maxTailers = maxTailers;
        return this;
    }

    /**
     * maxTailers
     *
     * @return number of tailers that will be required from this queue, not including the draining tailer
     */
    public int maxTailers() {
        return maxTailers;
    }

    public SingleChronicleQueueBuilder bufferBytesStoreCreator(ThrowingBiFunction<Long, Integer, BytesStore, Exception> bufferBytesStoreCreator) {
        this.bufferBytesStoreCreator = bufferBytesStoreCreator;
        return this;
    }

    /**
     * Creator for BytesStore for underlying ring buffer. Allows visibility of RB's data to be controlled.
     * See also RB_BYTES_STORE_CREATOR_NATIVE, RB_BYTES_STORE_CREATOR_MAPPED_FILE
     *
     * @return
     */
    @Nullable
    public ThrowingBiFunction<Long, Integer, BytesStore, Exception> bufferBytesStoreCreator() {
        return bufferBytesStoreCreator;
    }

    /**
     * Enable out-of-process pretoucher (AKA preloader) (Queue Enterprise feature)
     *
     * @param pretouchIntervalMillis
     * @return
     */
    public SingleChronicleQueueBuilder enablePreloader(final long pretouchIntervalMillis) {
        this.pretouchIntervalMillis = pretouchIntervalMillis;
        return this;
    }

    /**
     * Interval in ms to invoke out of process pretoucher. Default is not to turn on
     *
     * @return interval ms
     */
    public long pretouchIntervalMillis() {
        return pretouchIntervalMillis;
    }

    public boolean hasPretouchIntervalMillis() {
        return pretouchIntervalMillis != null;
    }

    public SingleChronicleQueueBuilder path(String path) {
        return path(new File(path));
    }

    public SingleChronicleQueueBuilder path(final File path) {
        this.path = path;
        return this;
    }

    public SingleChronicleQueueBuilder path(final Path path) {
        this.path = path.toFile();
        return this;
    }

    /**
     * consumer will be called every second, also as there is data to report
     *
     * @param onRingBufferStats a consumer of the BytesRingBufferStats
     * @return this
     */
    public SingleChronicleQueueBuilder onRingBufferStats(@NotNull Consumer<BytesRingBufferStats> onRingBufferStats) {
        this.onRingBufferStats = onRingBufferStats;
        return this;
    }

    @NotNull
    public Consumer<BytesRingBufferStats> onRingBufferStats() {
        return this.onRingBufferStats == null ? NoBytesRingBufferStats.NONE : onRingBufferStats;
    }

    @NotNull
    public File path() {
        return this.path;
    }

    public SingleChronicleQueueBuilder blockSize(long blockSize) {
        this.blockSize = Math.max(TEST_BLOCK_SIZE, blockSize);
        return this;
    }

    public SingleChronicleQueueBuilder blockSize(int blockSize) {
        return blockSize((long) blockSize);
    }

    public long blockSize() {

        long bs = blockSize == null ? OS.is64Bit() ? 64L << 20 : TEST_BLOCK_SIZE : blockSize;

        // can add an index2index & an index in one go.
        long minSize = Math.max(TEST_BLOCK_SIZE, 32L * indexCount());
        return Math.max(minSize, bs);
    }

    /**
     * THIS IS FOR TESTING ONLY.
     * This makes the block size small to speed up short tests and show up issues which occur when moving from one block to another.
     * <p>
     * Using this will be slower when you have many messages, and break when you have large messages.
     * </p>
     *
     * @return this
     */
    public SingleChronicleQueueBuilder testBlockSize() {
        // small size for testing purposes only.
        return blockSize(64 << 10);
    }

    @NotNull
    public SingleChronicleQueueBuilder wireType(@NotNull WireType wireType) {
        if (wireType == WireType.DELTA_BINARY)
            deltaCheckpointInterval(64);
        this.wireType = wireType;
        return this;
    }

    private void deltaCheckpointInterval(int deltaCheckpointInterval) {
        assert checkIsPowerOf2(deltaCheckpointInterval);
        this.deltaCheckpointInterval = deltaCheckpointInterval;
    }

    private boolean checkIsPowerOf2(long value) {
        return (value & (value - 1)) == 0;
    }

    @NotNull
    public WireType wireType() {
        return this.wireType == null ? WireType.BINARY_LIGHT : wireType;
    }

    @NotNull
    public SingleChronicleQueueBuilder rollCycle(@NotNull RollCycle rollCycle) {
        this.rollCycle = rollCycle;
        return this;
    }

    @NotNull
    public RollCycle rollCycle() {
        return this.rollCycle;
    }

    /**
     * @return ringBufferCapacity in bytes
     */
    public long bufferCapacity() {
        long bufferCapacity = this.bufferCapacity == null ? 0 : this.bufferCapacity;
        Long blockSize = blockSize();
        return Math.min(blockSize / 4, bufferCapacity == -1 ? 2 << 20 : Math.max(4 << 10,
                bufferCapacity));
    }

    /**
     * @param bufferCapacity sets the ring buffer capacity in bytes
     * @return this
     */
    @NotNull
    public SingleChronicleQueueBuilder bufferCapacity(long bufferCapacity) {
        this.bufferCapacity = bufferCapacity;
        return this;
    }

    /**
     * sets epoch offset in milliseconds
     *
     * @param epoch sets an epoch offset as the number of number of milliseconds since January 1,
     *              1970,  00:00:00 GMT
     * @return {@code this}
     */
    @NotNull
    public SingleChronicleQueueBuilder epoch(long epoch) {
        this.epoch = epoch;
        queueOffsetSpec = QueueOffsetSpec.ofEpoch(epoch);
        return this;
    }

    /**
     * @return epoch offset as the number of number of milliseconds since January 1, 1970,  00:00:00
     * GMT
     */
    public long epoch() {
        return epoch == null ? Long.getLong(DEFAULT_EPOCH_PROPERTY, 0L) : epoch;
    }

    /**
     * when set to {@code true}. uses a ring buffer to buffer appends, excerpts are written to the
     * Chronicle Queue using a background thread
     *
     * @param isBuffered {@code true} if the append is buffered
     * @return this
     */
    @NotNull
    @Deprecated
    public SingleChronicleQueueBuilder buffered(boolean isBuffered) {
        this.writeBufferMode = isBuffered ? BufferMode.Asynchronous : BufferMode.None;
        return this;
    }

    /**
     * @return if we uses a ring buffer to buffer the appends, the Excerpts are written to the
     * Chronicle Queue using a background thread
     */
    @Deprecated
    public boolean buffered() {
        return this.writeBufferMode == BufferMode.Asynchronous;
    }

    /**
     * @return BufferMode to use for writes. Only None is available is the OSS
     */
    @NotNull
    public BufferMode writeBufferMode() {
        return wireType() == WireType.DELTA_BINARY ? BufferMode.None : (writeBufferMode == null)
                ? BufferMode.None : writeBufferMode;
    }

    public SingleChronicleQueueBuilder writeBufferMode(BufferMode writeBufferMode) {
        this.writeBufferMode = writeBufferMode;
        return this;
    }

    /**
     * @return BufferMode to use for reads. Only None is available is the OSS
     */
    public BufferMode readBufferMode() {
        return readBufferMode == null ? BufferMode.None : readBufferMode;
    }

    public SingleChronicleQueueBuilder readBufferMode(BufferMode readBufferMode) {
        this.readBufferMode = readBufferMode;
        return this;
    }

    /**
     * @return a new event loop instance if none has been set, otherwise the {@code eventLoop}
     * that was set
     */
    @NotNull
    public EventLoop eventLoop() {
        return eventLoop == null ? new EventGroup(true) : eventLoop;
    }

    @NotNull
    public SingleChronicleQueueBuilder eventLoop(EventLoop eventLoop) {
        this.eventLoop = eventLoop;
        return this;
    }

    /**
     * @return if the ring buffer's monitoring capability is turned on. Not available in OSS
     */
    public boolean enableRingBufferMonitoring() {
        return enableRingBufferMonitoring == null ? false : enableRingBufferMonitoring;
    }

    public SingleChronicleQueueBuilder enableRingBufferMonitoring(boolean enableRingBufferMonitoring) {
        this.enableRingBufferMonitoring = enableRingBufferMonitoring;
        return this;
    }

    /**
     * @return if ring buffer reader processes can invoke the CQ drainer, otherwise only writer processes can
     */
    public boolean ringBufferReaderCanDrain() {
        return ringBufferReaderCanDrain == null ? false : ringBufferReaderCanDrain;
    }

    public SingleChronicleQueueBuilder ringBufferReaderCanDrain(boolean ringBufferReaderCanDrain) {
        this.ringBufferReaderCanDrain = ringBufferReaderCanDrain;
        return this;
    }

    public SingleChronicleQueueBuilder indexCount(int indexCount) {
        this.indexCount = Maths.nextPower2(indexCount, 8);
        return this;
    }

    public int indexCount() {
        return indexCount == null || indexCount <= 0 ? rollCycle().defaultIndexCount() : indexCount;
    }

    public SingleChronicleQueueBuilder indexSpacing(int indexSpacing) {
        this.indexSpacing = Maths.nextPower2(indexSpacing, 1);
        return this;
    }

    public int indexSpacing() {
        return indexSpacing == null || indexSpacing <= 0 ? rollCycle().defaultIndexSpacing() :
                indexSpacing;
    }

    public TimeProvider timeProvider() {
        return timeProvider == null ? SystemTimeProvider.INSTANCE : timeProvider;
    }

    public SingleChronicleQueueBuilder timeProvider(TimeProvider timeProvider) {
        this.timeProvider = timeProvider;
        return this;
    }

    public Supplier<TimingPauser> pauserSupplier() {
        return pauserSupplier;
    }

    public SingleChronicleQueueBuilder pauserSupplier(Supplier<TimingPauser> pauser) {
        this.pauserSupplier = pauser;
        return this;
    }

    public SingleChronicleQueueBuilder timeoutMS(long timeoutMS) {
        this.timeoutMS = timeoutMS;
        return this;
    }

    public long timeoutMS() {
        return timeoutMS == null ? 10_000L : timeoutMS;
    }

    public SingleChronicleQueueBuilder storeFileListener(StoreFileListener storeFileListener) {
        this.storeFileListener = storeFileListener;
        return this;
    }

    public StoreFileListener storeFileListener() {
        return storeFileListener == null ?
                (cycle, file) -> {
                    if (Jvm.isDebugEnabled(getClass()))
                        Jvm.debug().on(getClass(), "File released " + file);
                } : storeFileListener;

    }

    public SingleChronicleQueueBuilder sourceId(int sourceId) {
        if (sourceId < 0)
            throw new IllegalArgumentException("Invalid source Id, must be positive");
        this.sourceId = sourceId;
        return this;
    }

    public int sourceId() {
        return sourceId == null ? 0 : sourceId;
    }

    public boolean readOnly() {
        return readOnly == Boolean.TRUE && !OS.isWindows();
    }

    public SingleChronicleQueueBuilder readOnly(boolean readOnly) {
        if (OS.isWindows() && readOnly)
            Jvm.warn().on(SingleChronicleQueueBuilder.class,
                    "Read-only mode is not supported on Windows® platforms, defaulting to " +
                            "read/write.");
        else
            this.readOnly = readOnly;

        return this;
    }

    public Supplier<BiConsumer<BytesStore, Bytes>> encodingSupplier() {
        return encodingSupplier;
    }

    public Supplier<BiConsumer<BytesStore, Bytes>> decodingSupplier() {
        return decodingSupplier;
    }

    public SingleChronicleQueueBuilder codingSuppliers(@Nullable
                                                               Supplier<BiConsumer<BytesStore, Bytes>> encodingSupplier,
                                                       @Nullable Supplier<BiConsumer<BytesStore, Bytes>> decodingSupplier) {
        if ((encodingSupplier == null) != (decodingSupplier == null))
            throw new UnsupportedOperationException("Both encodingSupplier and decodingSupplier must be set or neither");
        this.encodingSupplier = encodingSupplier;
        this.decodingSupplier = decodingSupplier;
        return this;
    }

    public SecretKeySpec key() {
        return key;
    }

    protected void preBuild() {
        initializeMetadata();
    }

    public SingleChronicleQueueBuilder strongAppenders(boolean strongAppenders) {
        this.strongAppenders = strongAppenders;
        return this;
    }

    public boolean strongAppenders() {
        return Boolean.TRUE.equals(strongAppenders);
    }

    // *************************************************************************
    //
    // *************************************************************************

    public boolean checkInterrupts() {
        if (checkInterrupts == null) {
            if (System.getProperties().contains("chronicle.queue.ignoreInterrupts"))
                return !Boolean.getBoolean("chronicle.queue.ignoreInterrupts");
            if (System.getProperties().contains("chronicle.queue.checkInterrupts"))
                return Boolean.getBoolean("chronicle.queue.checkInterrupts");
        }

        // default is true unless turned off.
        return !Boolean.FALSE.equals(checkInterrupts);
    }

    public SingleChronicleQueueBuilder checkInterrupts(boolean checkInterrupts) {
        this.checkInterrupts = checkInterrupts;
        return this;
    }

    public SingleChronicleQueueBuilder clone() {
        try {
            return (SingleChronicleQueueBuilder) super.clone();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    /**
     * updates all the fields in {@code this} that are null, from the {@param parentBuilder}
     *
     * @param parentBuilder the parentBuilder Chronicle Queue Builder
     * @return that
     */

    public SingleChronicleQueueBuilder setAllNullFields(@Nullable SingleChronicleQueueBuilder parentBuilder) {
        if (parentBuilder == null)
            return this;

        if (!(this.getClass().isAssignableFrom(parentBuilder.getClass()) || parentBuilder.getClass().isAssignableFrom(this.getClass())))
            throw new IllegalArgumentException("Classes are not in same implementation hierarchy");

        List<FieldInfo> sourceFieldInfo = Wires.fieldInfos(parentBuilder.getClass());

        for (final FieldInfo fieldInfo : Wires.fieldInfos(this.getClass())) {
            if (!sourceFieldInfo.contains(fieldInfo))
                continue;
            Object resultV = fieldInfo.get(this);
            Object parentV = fieldInfo.get(parentBuilder);
            if (resultV == null && parentV != null)
                fieldInfo.set(this, parentV);

        }
        return this;
    }

    enum NoBytesRingBufferStats implements Consumer<BytesRingBufferStats> {
        NONE;

        @Override
        public void accept(BytesRingBufferStats bytesRingBufferStats) {
        }
    }
}