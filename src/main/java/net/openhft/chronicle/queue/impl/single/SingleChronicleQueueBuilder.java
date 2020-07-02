/*
 * Copyright 2016-2020 Chronicle Software
 *
 * https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
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
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.core.threads.OnDemandEventLoop;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.core.util.ObjectUtils;
import net.openhft.chronicle.core.util.ThrowingBiFunction;
import net.openhft.chronicle.core.util.Updater;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.queue.impl.WireStoreFactory;
import net.openhft.chronicle.queue.impl.table.ReadonlyTableStore;
import net.openhft.chronicle.queue.impl.table.SingleTableBuilder;
import net.openhft.chronicle.threads.MediumEventLoop;
import net.openhft.chronicle.threads.Pauser;
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
import java.time.format.DateTimeFormatter;
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

public class SingleChronicleQueueBuilder extends SelfDescribingMarshallable implements Cloneable {
    public static final String DEFAULT_ROLL_CYCLE_PROPERTY = "net.openhft.queue.builder.defaultRollCycle";
    private static final Constructor ENTERPRISE_QUEUE_CONSTRUCTOR;
    private static final String DEFAULT_EPOCH_PROPERTY = "net.openhft.queue.builder.defaultEpoch";
    private static final Logger LOGGER = LoggerFactory.getLogger(SingleChronicleQueueBuilder.class);

    private static final WireStoreFactory storeFactory = SingleChronicleQueueBuilder::createStore;
    private static final Supplier<TimingPauser> TIMING_PAUSER_SUPPLIER = DefaultPauserSupplier.INSTANCE;

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
            ENTERPRISE_QUEUE_CONSTRUCTOR = co;
        }
    }

    private BufferMode writeBufferMode = BufferMode.None;
    private BufferMode readBufferMode = BufferMode.None;
    private WireType wireType = WireType.BINARY_LIGHT;
    private Long blockSize;
    private File path;
    private RollCycle rollCycle;
    private Long epoch; // default is 1970-01-01 00:00:00.000 UTC
    private Long bufferCapacity;
    private Integer indexSpacing;
    private Integer indexCount;
    private Boolean enableRingBufferMonitoring;
    private Boolean ringBufferReaderCanDrain;
    private Boolean ringBufferForceCreateReader;
    private Boolean ringBufferReopenReader;
    private Supplier<Pauser> ringBufferPauserSupplier;
    private HandlerPriority drainerPriority;
    @Nullable
    private EventLoop eventLoop;
    /**
     * by default does not log any stats of the ring buffer
     */
    private Consumer<BytesRingBufferStats> onRingBufferStats;
    private TimeProvider timeProvider;
    private Supplier<TimingPauser> pauserSupplier;
    private Long timeoutMS; // 10 seconds.
    private Integer sourceId;
    private StoreFileListener storeFileListener;

    private Boolean readOnly;
    private Boolean strongAppenders;
    private Boolean checkInterrupts;

    private transient TableStore<SCQMeta> metaStore;

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
    private boolean doubleBuffer;

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
        MappedBytes mappedBytes = (MappedBytes) wire.bytes();
        final SingleChronicleQueueStore wireStore = new SingleChronicleQueueStore(
                queue.rollCycle(),
                queue.wireType(),
                mappedBytes,
                queue.indexCount(),
                queue.indexSpacing());

        wire.writeEventName(MetaDataKeys.header).typedMarshallable(wireStore);
        return wireStore;
    }

    private static boolean isQueueReplicationAvailable() {
        return ENTERPRISE_QUEUE_CONSTRUCTOR != null;
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
                        @SuppressWarnings("unchecked")
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
        if (ENTERPRISE_QUEUE_CONSTRUCTOR == null)
            LOGGER.warn(feature + " is only supported in Chronicle Queue Enterprise. If you would like to use this feature, please contact sales@chronicle.software for more information.");
        return true;
    }

    @NotNull
    private SingleChronicleQueue buildEnterprise() {
        if (ENTERPRISE_QUEUE_CONSTRUCTOR == null)
            throw new IllegalStateException("Enterprise features requested but Chronicle Queue Enterprise is not in the class path!");

        try {
            return (SingleChronicleQueue) ENTERPRISE_QUEUE_CONSTRUCTOR.newInstance(this);
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

            String format = newMeta.roll().format();
            if (!format.equals(rollCycle().format())) {
                // roll cycle changed
                overrideRollCycleForFileName(format);
            }

            // if it was overridden - reset
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
                for (RollCycles cycle : RollCycles.all()) {
                    try {
                        DateTimeFormatter.ofPattern(cycle.format())
                                .parse(filename.substring(0, filename.length() - 4));
                        overrideRollCycle(cycle);
                    } catch (Exception expected) {
                    }
                }
            }
        }
    }

    private void overrideRollCycleForFileName(String pattern) {
        for (RollCycles cycle : RollCycles.all()) {
            if (cycle.format().equals(pattern)) {
                overrideRollCycle(cycle);
                return;
            }
        }
        throw new IllegalStateException("Can't find an appropriate RollCycles to override to of length " + pattern);
    }

    private void overrideRollCycle(RollCycles cycle) {
        LOGGER.warn("Overriding roll cycle from {} to {}", rollCycle, cycle);
        rollCycle = cycle;
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
     * See also EnterpriseSingleChronicleQueue.RB_BYTES_STORE_CREATOR_NATIVE, EnterpriseSingleChronicleQueue.RB_BYTES_STORE_CREATOR_MAPPED_FILE
     *
     * @return bufferBytesStoreCreator
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

    /**
     * @return - this is the size of a memory mapping chunk, a queue is read/written by using a number of blocks, you should avoid changing this unnecessarily.
     */
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
        assert rollCycle != null;
        this.rollCycle = rollCycle;
        return this;
    }

    @NotNull
    public RollCycle rollCycle() {
        return this.rollCycle == null ? loadDefaultRollCycle() : this.rollCycle;
    }

    /**
     * @return ring buffer capacity in bytes [ Chronicle-Ring is an enterprise product ]
     */
    public long bufferCapacity() {
        return Math.min(blockSize() / 4, bufferCapacity == null ? 2 << 20 : bufferCapacity);
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
     * Chronicle Queue using a background thread. See also {@link #writeBufferMode()}
     *
     * @param isBuffered {@code true} if the append is buffered
     * @return this
     */
    @NotNull
    @Deprecated // use writeBufferMode(Asynchronous) instead
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

    /**
     * When writeBufferMode is set to {@code Asynchronous}, uses a ring buffer to buffer appends, excerpts are written to the
     * Chronicle Queue using a background thread.
     * See also {@link #bufferCapacity()}
     * See also {@link #bufferBytesStoreCreator()}
     * See also software.chronicle.enterprise.ring.EnterpriseRingBuffer
     *
     * @param writeBufferMode bufferMode for writing
     * @return this
     */
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

    /**
     * When readBufferMode is set to {@code Asynchronous}, reads from the ring buffer. This requires
     * that {@link #writeBufferMode()} is also set to {@code Asynchronous}.
     * See also {@link #bufferCapacity()}
     * See also {@link #bufferBytesStoreCreator()}
     * See also software.chronicle.enterprise.ring.EnterpriseRingBuffer
     *
     * @param readBufferMode BufferMode for read
     * @return this
     */
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
        if (eventLoop == null)
            return new OnDemandEventLoop(
                    () -> new MediumEventLoop(null, path.getName(), Pauser.busy(), true, "none"));
        return eventLoop;
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
        return ringBufferReaderCanDrain == null ? true : ringBufferReaderCanDrain;
    }

    public SingleChronicleQueueBuilder ringBufferReaderCanDrain(boolean ringBufferReaderCanDrain) {
        this.ringBufferReaderCanDrain = ringBufferReaderCanDrain;
        return this;
    }

    /**
     * @return whether to force creating a reader (to recover from crash)
     */
    public boolean ringBufferForceCreateReader() {
        return ringBufferForceCreateReader == null ? false : ringBufferForceCreateReader;
    }

    public SingleChronicleQueueBuilder ringBufferForceCreateReader(boolean ringBufferForceCreateReader) {
        this.ringBufferForceCreateReader = ringBufferForceCreateReader;
        return this;
    }

    /**
     * @return if ring buffer readers are not reset on close. If true then re-opening a reader puts you back
     * at the same place. If true, your reader can block writers if the reader is not open
     */
    public boolean ringBufferReopenReader() {
        return ringBufferReopenReader == null ? false : ringBufferReopenReader;
    }

    public SingleChronicleQueueBuilder ringBufferReopenReader(boolean ringBufferReopenReader) {
        this.ringBufferReopenReader = ringBufferReopenReader;
        return this;
    }

    /**
     * Priority for ring buffer's drainer handler
     *
     * @return drainerPriority
     */
    public HandlerPriority drainerPriority() {
        return drainerPriority == null ? HandlerPriority.MEDIUM : drainerPriority;
    }

    public SingleChronicleQueueBuilder drainerPriority(HandlerPriority drainerPriority) {
        this.drainerPriority = drainerPriority;
        return this;
    }

    /**
     * Pauser supplier for the pauser to be used by ring buffer when waiting
     */
    public Supplier<Pauser> ringBufferPauserSupplier() {
        return ringBufferPauserSupplier == null ? Pauser::busy : ringBufferPauserSupplier;
    }

    @Deprecated
    public SingleChronicleQueueBuilder ringBufferPauser(Pauser ringBufferPauser) {
        return ringBufferPauserSupplier(() -> ringBufferPauser);
    }

    public SingleChronicleQueueBuilder ringBufferPauserSupplier(Supplier<Pauser> ringBufferPauserSupplier) {
        this.ringBufferPauserSupplier = ringBufferPauserSupplier;
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
        return pauserSupplier == null ? TIMING_PAUSER_SUPPLIER : pauserSupplier;
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
                    "Read-only mode is not supported on Windows® platforms, defaulting to read/write.");
        else
            this.readOnly = readOnly;

        return this;
    }

    public boolean doubleBuffer() {
        return doubleBuffer;
    }

    /**
     * <p>
     * Enables double-buffered writes on contention.
     * </p><p>
     * Normally, all writes to the queue will be serialized based on the write lock acquisition. Each time {@link ExcerptAppender#writingDocument()}
     * is called, appender tries to acquire the write lock on the queue, and if it fails to do so it blocks until write
     * lock is unlocked, and in turn locks the queue for itself.
     * </p><p>
     * When double-buffering is enabled, if appender sees that the write lock is acquired upon {@link ExcerptAppender#writingDocument()} call,
     * it returns immediately with a context pointing to the secondary buffer, and essentially defers lock acquisition
     * until the context.close() is called (normally with try-with-resources pattern it is at the end of the try block),
     * allowing user to go ahead writing data, and then essentially doing memcpy on the serialized data (thus reducing cost of serialization).
     * </p><p>
     * This is only useful if (majority of) the objects being written to the queue are big enough AND their marshalling is not straight-forward
     * (e.g. BytesMarshallable's marshalling is very efficient and quick and hence double-buffering will only slow things down), and if there's a
     * heavy contention on writes (e.g. 2 or more threads writing a lot of data to the queue at a very high rate).
     * </p>
     */
    public SingleChronicleQueueBuilder doubleBuffer(boolean doubleBuffer) {
        this.doubleBuffer = doubleBuffer;
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
        try {
            initializeMetadata();
        } catch (Exception ex) {
            metaStore.close();
            throw ex;
        }
    }

    /**
     * @param strongAppenders by default, we create the appenders as a weak reference, these can get garbage collected. To avoid them becoming unnecessarily garbage collected, set this to {@code true}
     * @return that
     */
    public SingleChronicleQueueBuilder strongAppenders(boolean strongAppenders) {
        this.strongAppenders = strongAppenders;
        return this;
    }

    /**
     * @return {@code true} if we are using strong appender, by default, we create the appenders in a thread-local, weak reference, these can get
     * garbage collected. * Setting them to strong will ensure they are created using a strong reference.
     */
    public boolean strongAppenders() {
        return Boolean.TRUE.equals(strongAppenders);
    }

    // *************************************************************************
    //
    // *************************************************************************

    public boolean checkInterrupts() {
        if (checkInterrupts == null) {
            if (System.getProperties().contains("chronicle.queue.ignoreInterrupts"))
                return !Jvm.getBoolean("chronicle.queue.ignoreInterrupts");
            if (System.getProperties().contains("chronicle.queue.checkInterrupts"))
                return Jvm.getBoolean("chronicle.queue.checkInterrupts");
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
     * updates all the fields in <code>this</code> that are null, from the parentBuilder
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

    enum DefaultPauserSupplier implements Supplier<TimingPauser> {
        INSTANCE;

        @Override
        public TimingPauser get() {
            return new TimeoutPauser(500_000);
        }
    }
}