/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.*;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.core.threads.OnDemandEventLoop;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.core.util.Builder;
import net.openhft.chronicle.core.util.ObjectUtils;
import net.openhft.chronicle.core.util.Updater;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.*;
import net.openhft.chronicle.queue.impl.table.ReadonlyTableStore;
import net.openhft.chronicle.queue.impl.table.SingleTableBuilder;
import net.openhft.chronicle.queue.internal.domestic.QueueOffsetSpec;
import net.openhft.chronicle.threads.MediumEventLoop;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.threads.TimingPauser;
import net.openhft.chronicle.threads.YieldingPauser;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.lang.reflect.Constructor;
import java.nio.file.Path;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static net.openhft.chronicle.core.pool.ClassAliasPool.CLASS_ALIASES;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueue.QUEUE_METADATA_FILE;

/**
 * Builder class for creating instances of {@link SingleChronicleQueue}.
 * This class provides various configuration options such as buffer modes, wire type,
 * roll cycle, and event loop settings. It is used to construct a queue with
 * customized settings.
 *
 * <p>This class uses the builder pattern to allow flexible configuration and
 * creation of {@link SingleChronicleQueue} instances.
 */
public class SingleChronicleQueueBuilder extends SelfDescribingMarshallable implements Cloneable, Builder<SingleChronicleQueue> {
    public static final long SMALL_BLOCK_SIZE = OS.isWindows() ? OS.SAFE_PAGE_SIZE : OS.pageSize(); // the smallest safe block size on Windows 8+
    static final boolean DEBUG_FILE_RELEASED = Jvm.getBoolean("debug.file.released", false);
    private static final long DEFAULT_BLOCK_SIZE = Math.min(
            Jvm.getSize("SingleChronicleQueueBuilder.blocksize", OS.is64Bit() ? 64L << 20 : SMALL_BLOCK_SIZE),
            OS.is64Bit() && OS.isLinux() ? Long.MAX_VALUE : 256L << 20); // 256MB on 32-bit or non-Linux

    public static final long DEFAULT_SPARSE_CAPACITY = 512L << 30;
    private static final Constructor<?> ENTERPRISE_QUEUE_CONSTRUCTOR;
    private static final WireStoreFactory storeFactory = SingleChronicleQueueBuilder::createStore;
    private static final Supplier<TimingPauser> TIMING_PAUSER_SUPPLIER = DefaultPauserSupplier.INSTANCE;

    static {
        CLASS_ALIASES.addAlias(WireType.class);
        CLASS_ALIASES.addAlias(SCQMeta.class, "SCQMeta");
        CLASS_ALIASES.addAlias(SCQRoll.class, "SCQSRoll");
        CLASS_ALIASES.addAlias(SCQIndexing.class, "SCQSIndexing");
        CLASS_ALIASES.addAlias(SingleChronicleQueueStore.class, "SCQStore");

        Constructor<?> co;
        try {
            co = ((Class<?>) Class.forName("software.chronicle.enterprise.queue.EnterpriseSingleChronicleQueue")).getDeclaredConstructors()[0];
            Jvm.setAccessible(co);
        } catch (Exception e) {
            co = null;
        }
        ENTERPRISE_QUEUE_CONSTRUCTOR = co;
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
    private HandlerPriority drainerPriority = HandlerPriority.CONCURRENT;
    private int drainerTimeoutMS = -1;

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
    private boolean checkInterrupts;

    private transient TableStore<SCQMeta> metaStore;

    // enterprise stuff
    private Supplier<BiConsumer<BytesStore<?, ?>, Bytes<?>>> encodingSupplier;
    private Supplier<BiConsumer<BytesStore<?, ?>, Bytes<?>>> decodingSupplier;
    private Updater<Bytes<?>> messageInitializer;
    private Consumer<Bytes<?>> messageHeaderReader;
    private SecretKeySpec key;

    private int maxTailers;
    private AsyncBufferCreator bufferBytesStoreCreator;
    private Long pretouchIntervalMillis;
    private LocalTime rollTime;
    private ZoneId rollTimeZone;
    private QueueOffsetSpec queueOffsetSpec;
    private boolean doubleBuffer;
    private Function<SingleChronicleQueue, Condition> createAppenderConditionCreator;
    private long forceDirectoryListingRefreshIntervalMs = 60_000;
    private AppenderListener appenderListener;
    private SyncMode syncMode;

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
     * Creates a new builder instance for configuring a {@link SingleChronicleQueue}.
     *
     * @return a new instance of SingleChronicleQueueBuilder
     */
    public static SingleChronicleQueueBuilder builder() {
        return new SingleChronicleQueueBuilder();
    }

    /**
     * Creates a new builder instance with the specified path and wire type.
     *
     * @param path     the path to the queue directory
     * @param wireType the wire type for serialization
     * @return a new instance of SingleChronicleQueueBuilder
     */
    @NotNull
    public static SingleChronicleQueueBuilder builder(@NotNull Path path, @NotNull WireType wireType) {
        return builder(path.toFile(), wireType);
    }

    /**
     * Creates a new builder instance with the specified file and wire type.
     * If the file is a specific queue file, a warning is logged, and the parent directory is used as the path.
     * Otherwise, the provided directory is used.
     *
     * @param file     the file or directory to be used for the queue
     * @param wireType the wire type for serialization
     * @return a configured {@link SingleChronicleQueueBuilder} instance
     * @throws IllegalArgumentException if the file is not a valid queue file
     */
    @NotNull
    public static SingleChronicleQueueBuilder builder(@NotNull File file, @NotNull WireType wireType) {
        SingleChronicleQueueBuilder result = builder().wireType(wireType);

        // If the file is a specific queue file, warn the user and use the parent directory
        if (file.isFile()) {
            if (!file.getName().endsWith(SingleChronicleQueue.SUFFIX)) {
                throw new IllegalArgumentException("Invalid file type: " + file.getName());
            }

            Jvm.warn().on(SingleChronicleQueueBuilder.class,
                    "Queues should be configured with the queue directory, not a specific filename. Actual file used: "
                            + file.getParentFile());

            result.path(file.getParentFile());
        } else {
            result.path(file);
        }
        return result;
    }

    /**
     * Creates a new builder with the default wire type of {@link WireType#BINARY_LIGHT}.
     *
     * @return a {@link SingleChronicleQueueBuilder} instance with binary wire type
     */
    public static SingleChronicleQueueBuilder single() {
        SingleChronicleQueueBuilder builder = builder();
        builder.wireType(WireType.BINARY_LIGHT);
        return builder;
    }

    /**
     * Creates a new builder with a binary wire type for the specified base path.
     *
     * @param basePath the base path for the queue
     * @return a {@link SingleChronicleQueueBuilder} instance
     */
    public static SingleChronicleQueueBuilder single(@NotNull String basePath) {
        return binary(basePath);
    }

    /**
     * Creates a new builder with a binary wire type for the specified base path as a {@link File}.
     *
     * @param basePath the base path for the queue
     * @return a {@link SingleChronicleQueueBuilder} instance
     */
    public static SingleChronicleQueueBuilder single(@NotNull File basePath) {
        return binary(basePath);
    }

    /**
     * Creates a new builder with a binary wire type for the specified path as a {@link Path}.
     *
     * @param path the path for the queue
     * @return a {@link SingleChronicleQueueBuilder} instance
     */
    public static SingleChronicleQueueBuilder binary(@NotNull Path path) {
        return binary(path.toFile());
    }

    /**
     * Creates a new builder with a binary wire type for the specified base path.
     *
     * @param basePath the base path for the queue
     * @return a {@link SingleChronicleQueueBuilder} instance
     */
    public static SingleChronicleQueueBuilder binary(@NotNull String basePath) {
        return binary(new File(basePath));
    }

    /**
     * Creates a new builder with a binary wire type for the specified base path as a {@link File}.
     *
     * @param basePathFile the base path for the queue
     * @return a {@link SingleChronicleQueueBuilder} instance
     */
    public static SingleChronicleQueueBuilder binary(@NotNull File basePathFile) {
        return builder(basePathFile, WireType.BINARY_LIGHT);
    }

    /**
     * Creates a new {@link SingleChronicleQueueStore} for the given queue and wire.
     * This method initializes the store with the appropriate configuration and writes the header.
     *
     * @param queue the queue for which the store is created
     * @param wire  the wire to be used for the store
     * @return a newly created {@link SingleChronicleQueueStore}
     */
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

    /**
     * Checks if enterprise features are available.
     *
     * @return true if enterprise features are available, false otherwise
     */
    public static boolean areEnterpriseFeaturesAvailable() {
        return ENTERPRISE_QUEUE_CONSTRUCTOR != null;
    }

    /**
     * Loads the default roll cycle based on the system property {@code QueueSystemProperties.DEFAULT_ROLL_CYCLE_PROPERTY}.
     * If no property is set, the default roll cycle is {@link RollCycles#DEFAULT}.
     *
     * @return the default {@link RollCycle}
     */
    private static RollCycle loadDefaultRollCycle() {
        String rollCycleProperty = Jvm.getProperty(QueueSystemProperties.DEFAULT_ROLL_CYCLE_PROPERTY);
        if (null == rollCycleProperty) {
            return RollCycles.DEFAULT;
        }

        String[] rollCyclePropertyParts = rollCycleProperty.split(":");
        if (rollCyclePropertyParts.length > 0) {
            try {
                Class<?> rollCycleClass = Class.forName(rollCyclePropertyParts[0]);
                if (Enum.class.isAssignableFrom(rollCycleClass)) {
                    // Handle roll cycle as an enum
                    if (rollCyclePropertyParts.length < 2) {
                        Jvm.warn().on(SingleChronicleQueueBuilder.class,
                                "Default roll cycle configured as enum, but enum value not specified: " + rollCycleProperty);
                    } else {
                        @SuppressWarnings({"unchecked", "rawtypes"})
                        Class<Enum> eClass = (Class<Enum>) rollCycleClass;
                        @SuppressWarnings("unchecked")
                        Object instance = ObjectUtils.valueOfIgnoreCase(eClass, rollCyclePropertyParts[1]);
                        if (instance instanceof RollCycle) {
                            return (RollCycle) instance;
                        } else {
                            Jvm.warn().on(SingleChronicleQueueBuilder.class,
                                    "Configured default rollcycle is not a subclass of RollCycle");
                        }
                    }
                } else {
                    // Handle roll cycle as a class instance
                    @SuppressWarnings("unchecked")
                    Object instance = ObjectUtils.newInstance(rollCycleClass);
                    if (instance instanceof RollCycle) {
                        return (RollCycle) instance;
                    } else {
                        Jvm.warn().on(SingleChronicleQueueBuilder.class,
                                "Configured default rollcycle is not a subclass of RollCycle");
                    }
                }
            } catch (ClassNotFoundException ignored) {
                Jvm.warn().on(SingleChronicleQueueBuilder.class,
                        "Default roll cycle class: " + rollCyclePropertyParts[0] + " was not found");
            }
        }

        return RollCycles.DEFAULT;
    }

    /**
     * Returns the store factory used to create wire stores.
     *
     * @return the {@link WireStoreFactory}
     */
    public WireStoreFactory storeFactory() {
        return storeFactory;
    }

    /**
     * Builds a new instance of {@link SingleChronicleQueue} based on the current configuration.
     * This method first checks for enterprise feature requests, builds the appropriate type of queue,
     * and performs any post-build tasks.
     *
     * @return a configured {@link SingleChronicleQueue} instance
     * @throws IllegalStateException if enterprise features are requested but not available
     */
    @NotNull
    public SingleChronicleQueue build() {
        preBuild();

        SingleChronicleQueue chronicleQueue;

        // It is important to check enterprise features after preBuild()
        // Enterprise-only config options can be loaded from the metadata
        if (checkEnterpriseFeaturesRequested())
            chronicleQueue = buildEnterprise();
        else
            chronicleQueue = new SingleChronicleQueue(this);

        postBuild(chronicleQueue);

        return chronicleQueue;
    }

    /**
     * Performs post-build tasks such as setting the appender condition.
     * The condition is added after the queue is constructed to avoid circular dependencies
     * by passing `this` during construction.
     *
     * @param chronicleQueue the queue that was just built
     */
    private void postBuild(@NotNull SingleChronicleQueue chronicleQueue) {
        if (!readOnly()) {
            /*
                The condition has a circular dependency with the Queue, so we need to add it after the queue is
                constructed. This is to avoid passing `this` out of the constructor.
             */
            chronicleQueue.createAppenderCondition(requireNonNull(createAppenderConditionCreator().apply(chronicleQueue)));
        }
    }

    /**
     * Checks if any enterprise-only features have been requested.
     * If such features are requested, this method logs a warning and indicates that the
     * enterprise version of Chronicle Queue should be used.
     *
     * @return true if enterprise features were requested, false otherwise
     */
    private boolean checkEnterpriseFeaturesRequested() {

        boolean result = false;
        if (readBufferMode != BufferMode.None)
            result = onlyAvailableInEnterprise("Buffering");
        if (writeBufferMode != BufferMode.None)
            result = onlyAvailableInEnterprise("Buffering");
        if (rollTimeZone != null && !rollTimeZone.getId().equals("UTC") && !rollTimeZone.getId().equals("Z"))
            result = onlyAvailableInEnterprise("Non-UTC roll time zone");
        if (encodingSupplier != null)
            result = onlyAvailableInEnterprise("Encoding");
        if (key != null)
            result = onlyAvailableInEnterprise("Encryption");
        if (hasPretouchIntervalMillis())
            result = onlyAvailableInEnterprise("Out of process pretouching");

        return result;
    }

    /**
     * Logs a warning if a feature is only available in the enterprise version of Chronicle Queue.
     *
     * @param feature the name of the feature being requested
     * @return true, indicating that the feature is enterprise-only
     */
    public static boolean onlyAvailableInEnterprise(final String feature) {
        if (ENTERPRISE_QUEUE_CONSTRUCTOR == null)
            Jvm.warn().on(SingleChronicleQueueBuilder.class, feature + " is only supported in Chronicle Queue Enterprise. If you would like to use this feature, please contact sales@chronicle.software for more information.");
        return true;
    }

    /**
     * Builds and returns an instance of the enterprise version of {@link SingleChronicleQueue}.
     * Throws an {@link IllegalStateException} if enterprise features are requested but the enterprise version
     * is not available in the classpath.
     *
     * @return an instance of {@link SingleChronicleQueue} for enterprise use
     * @throws IllegalStateException if enterprise features are requested but unavailable
     */
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

    /**
     * Configures AES encryption using the provided key bytes. If the key is null, disables encryption.
     *
     * @param keyBytes the encryption key bytes, or null to disable encryption
     * @return the current builder instance for method chaining
     */
    public SingleChronicleQueueBuilder aesEncryption(@Nullable byte[] keyBytes) {
        if (keyBytes == null) {
            codingSuppliers(null, null); // Disable encryption if no key is provided
            return this;
        }
        key = new SecretKeySpec(keyBytes, "AES"); // Set the encryption key
        return this;
    }

    /**
     * Returns the message initializer. If no initializer is set, it defaults to clearing the bytes.
     *
     * @return the message initializer
     */
    public Updater<Bytes<?>> messageInitializer() {
        return messageInitializer == null ? Bytes::clear : messageInitializer;
    }

    /**
     * Returns the message header reader. If no reader is set, it defaults to a no-op.
     *
     * @return the message header reader
     */
    public Consumer<Bytes<?>> messageHeaderReader() {
        return messageHeaderReader == null ? b -> {
        } : messageHeaderReader;
    }

    /**
     * Sets the message initializer and header reader for configuring message headers.
     *
     * @param messageInitializer  the initializer to set up the message
     * @param messageHeaderReader the reader for the message header
     * @return the current builder instance for method chaining
     */
    public SingleChronicleQueueBuilder messageHeader(Updater<Bytes<?>> messageInitializer,
                                                     Consumer<Bytes<?>> messageHeaderReader) {
        this.messageInitializer = messageInitializer;
        this.messageHeaderReader = messageHeaderReader;
        return this;
    }

    /**
     * Sets the roll time for the queue using the provided {@link LocalTime}.
     *
     * @param rollTime the roll time to set
     * @return the current builder instance for method chaining
     */
    public SingleChronicleQueueBuilder rollTime(@NotNull final LocalTime rollTime) {
        rollTime(rollTime, rollTimeZone);
        return this;
    }

    /**
     * Returns the current roll time zone.
     *
     * @return the roll time zone
     */
    public ZoneId rollTimeZone() {
        return rollTimeZone;
    }

    /**
     * Sets the roll time zone for the queue using the provided {@link ZoneId}.
     *
     * @param rollTimeZone the time zone to set
     * @return the current builder instance for method chaining
     */
    public SingleChronicleQueueBuilder rollTimeZone(@NotNull final ZoneId rollTimeZone) {
        rollTime(rollTime, rollTimeZone);
        return this;
    }

    /**
     * Sets the roll time and time zone for the queue using the provided {@link LocalTime} and {@link ZoneId}.
     *
     * @param rollTime the roll time to set
     * @param zoneId   the time zone to set
     * @return the current builder instance for method chaining
     */
    public SingleChronicleQueueBuilder rollTime(@NotNull final LocalTime rollTime, @NotNull final ZoneId zoneId) {
        this.rollTime = rollTime;
        this.rollTimeZone = zoneId;
        this.epoch = TimeUnit.SECONDS.toMillis(rollTime.toSecondOfDay());
        this.queueOffsetSpec = QueueOffsetSpec.ofRollTime(rollTime, zoneId);
        return this;
    }

    /**
     * Initializes the metadata for the queue, including roll cycle and metadata overrides.
     * If in read-only mode and the metadata file is not found, falls back to a read-only table store.
     */
    protected void initializeMetadata() {
        File metapath = metapath();
        validateRollCycle(metapath);
        SCQMeta metadata = new SCQMeta(new SCQRoll(rollCycle(), epoch(), rollTime, rollTimeZone),
                sourceId());
        try {

            boolean readOnly = readOnly();
            metaStore = SingleTableBuilder.binary(metapath, metadata).readOnly(readOnly).build();
            // check if metadata was overridden
            SCQMeta newMeta = metaStore.metadata();
            sourceId(newMeta.sourceId());

            // If the roll cycle has been overridden, adjust it
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
            if (ex.getMessage().equals("Metadata file not found in readOnly mode"))
                Jvm.warn().on(getClass(), "Failback to readonly tablestore " + ex);
            else
                Jvm.warn().on(getClass(), "Failback to readonly tablestore", ex);

            // Fallback to read-only table store if metadata file is not found
            metaStore = new ReadonlyTableStore<>(metadata);
        }
    }

    /**
     * Validates the roll cycle by checking the existence of metadata. If no metadata is found,
     * it attempts to determine the roll cycle based on existing queue files in the directory.
     * This method cannot validate certain larger roll cycles.
     *
     * <p>For specific roll cycles like LARGE_HOURLY_SPARSE, LARGE_HOURLY_XSPARSE, LARGE_DAILY,
     * XLARGE_DAILY, HUGE_DAILY, and HUGE_DAILY_XSPARSE, the correct roll cycle must be manually
     * provided during queue creation.
     *
     * @param metapath the metadata path
     */
    private void validateRollCycle(File metapath) {
        if (!metapath.exists()) {
            // no metadata, so we need to check if there're cq4 files and if so try to validate roll cycle
            // the code is slightly brutal and crude but should work for most cases. It will NOT work if files were created with
            // the following cycles: LARGE_HOURLY_SPARSE LARGE_HOURLY_XSPARSE LARGE_DAILY XLARGE_DAILY HUGE_DAILY HUGE_DAILY_XSPARSE
            // for such cases user MUST use correct roll cycle when creating the queue
            String[] list = path.list((d, name) -> name.endsWith(SingleChronicleQueue.SUFFIX));
            if (list != null && list.length > 0) {
                // Try to match the roll cycle by parsing the filename against known roll cycles
                String filename = list[0];
                for (RollCycle cycle : RollCycles.all()) {
                    try {
                        // Attempt to parse the filename using the cycle's format
                        DateTimeFormatter.ofPattern(cycle.format())
                                .parse(filename.substring(0, filename.length() - 4));
                        overrideRollCycle(cycle);
                        break;
                    } catch (Exception expected) {
                        // Ignore the exception and continue checking other cycles
                    }
                }
            }
        }
    }

    /**
     * Overrides the roll cycle using the given pattern from the metadata. If no matching roll cycle
     * is found, an exception is thrown.
     *
     * @param pattern the roll cycle pattern from metadata
     * @throws IllegalStateException if no matching roll cycle is found
     */
    private void overrideRollCycleForFileName(String pattern) {
        for (RollCycle cycle : RollCycles.all()) {
            if (cycle.format().equals(pattern)) {
                overrideRollCycle(cycle);
                return;
            }
        }
        throw new IllegalStateException("Can't find an appropriate RollCycles to override to of length " + pattern);
    }

    /**
     * Sets the roll cycle to the specified {@link RollCycle}. Logs a warning if the roll cycle is being overridden.
     *
     * @param cycle the roll cycle to override to
     */
    private void overrideRollCycle(RollCycle cycle) {
        if (rollCycle != cycle && rollCycle != null)
            Jvm.warn().on(getClass(), "Overriding roll cycle from " + rollCycle + " to " + cycle);
        rollCycle = cycle;
    }

    /**
     * Constructs the metadata file path based on the queue's directory. Creates the directory if it does not exist.
     *
     * @return the file path for the queue's metadata
     */
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

    /**
     * Returns a factory for creating the {@link Condition} that will be used before creating a new appender.
     * If no condition creator has been set, returns a no-op condition.
     *
     * @return a {@link Function} that creates a {@link Condition} for the appender
     */
    @NotNull
    public Function<SingleChronicleQueue, Condition> createAppenderConditionCreator() {
        if (createAppenderConditionCreator == null) {
            return q -> NoOpCondition.INSTANCE;
        }
        return createAppenderConditionCreator;
    }

    /**
     * @return Factory for the {@link Condition} that will be waited on before a new appender is created
     * <p>
     * NOTE: The returned {@link Condition} will not block subsequent calls to acquireAppender from the
     * same thread, only when the call would result in the creation of a new appender.
     */
    @NotNull
    public SingleChronicleQueueBuilder createAppenderConditionCreator(Function<SingleChronicleQueue, Condition> creator) {
        createAppenderConditionCreator = creator;
        return this;
    }

    /**
     * Returns the write lock for this queue. If the queue is in read-only mode, a read-only lock is returned.
     * Otherwise, a {@link TableStoreWriteLock} is used for locking writes.
     *
     * @return the write lock for the queue
     */
    @NotNull
    WriteLock writeLock() {
        return readOnly() ? new ReadOnlyWriteLock() : new TableStoreWriteLock(metaStore, pauserSupplier(), timeoutMS() * 3 / 2);
    }

    /**
     * Returns the queue offset specification. If not set, returns a specification that indicates no offset.
     *
     * @return the queue offset specification
     */
    public QueueOffsetSpec queueOffsetSpec() {
        return queueOffsetSpec == null ? QueueOffsetSpec.ofNone() : queueOffsetSpec;
    }

    /**
     * Returns the metadata store for the queue.
     *
     * @return the {@link TableStore} containing the queue's metadata
     */
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
     * Returns the maximum number of tailers that will be required for this queue, excluding the draining tailer.
     *
     * @return the maximum number of tailers
     */
    public int maxTailers() {
        return maxTailers;
    }

    /**
     * Sets the asynchronous buffer creator for the queue. This is used in asynchronous mode
     * to control data visibility between processes or threads.
     *
     * @param asyncBufferCreator the buffer creator for asynchronous mode
     * @return the current builder instance for method chaining
     */
    public SingleChronicleQueueBuilder asyncBufferCreator(AsyncBufferCreator asyncBufferCreator) {
        this.bufferBytesStoreCreator = asyncBufferCreator;
        return this;
    }

    /**
     * Creator for BytesStore for async mode. Allows visibility of data to be controlled.
     * See also EnterpriseSingleChronicleQueue.RB_BYTES_STORE_CREATOR_NATIVE etc.
     * <p>
     * If you are using more than one {@link ChronicleQueue} object to access the async'd queue then you
     * will need to set this.
     * <p>
     * This is an enterprise feature.
     *
     * @return asyncBufferCreator
     */
    public AsyncBufferCreator asyncBufferCreator() {
        return bufferBytesStoreCreator;
    }

    /**
     * Enables the preloader (also known as the pretoucher) for out-of-process use. This is an enterprise feature.
     * The preloader will preload data into memory at regular intervals.
     *
     * @param pretouchIntervalMillis the interval in milliseconds between preload operations
     * @return the current builder instance for method chaining
     */
    public SingleChronicleQueueBuilder enablePreloader(final long pretouchIntervalMillis) {
        this.pretouchIntervalMillis = pretouchIntervalMillis;
        return this;
    }

    /**
     * Returns the interval in milliseconds to invoke the out-of-process pretoucher.
     * By default, this is not enabled unless explicitly configured.
     *
     * @return the pretouch interval in milliseconds
     */
    public long pretouchIntervalMillis() {
        return pretouchIntervalMillis;
    }

    /**
     * Checks if a pretouch interval has been set for the queue.
     *
     * @return true if the pretouch interval is set, false otherwise
     */
    public boolean hasPretouchIntervalMillis() {
        return pretouchIntervalMillis != null;
    }

    /**
     * Sets the path for the queue using a string representation of the path.
     *
     * @param path the path as a string
     * @return the current builder instance for method chaining
     */
    public SingleChronicleQueueBuilder path(String path) {
        return path(new File(path));
    }

    /**
     * Sets the path for the queue using a {@link File} object.
     *
     * @param path the file representing the path
     * @return the current builder instance for method chaining
     */
    public SingleChronicleQueueBuilder path(final File path) {
        this.path = path;
        return this;
    }

    /**
     * Sets the path for the queue using a {@link Path} object.
     *
     * @param path the path object
     * @return the current builder instance for method chaining
     */
    public SingleChronicleQueueBuilder path(final Path path) {
        this.path = path.toFile();
        return this;
    }

    /**
     * Sets a consumer to be called every second or when there is data to report regarding
     * ring buffer statistics.
     *
     * @param onRingBufferStats the consumer of {@link BytesRingBufferStats}
     * @return the current builder instance for method chaining
     */
    public SingleChronicleQueueBuilder onRingBufferStats(@NotNull Consumer<BytesRingBufferStats> onRingBufferStats) {
        this.onRingBufferStats = onRingBufferStats;
        return this;
    }

    /**
     * Returns the consumer for ring buffer statistics that was set for this builder.
     *
     * @return the consumer of {@link BytesRingBufferStats}
     */
    public Consumer<BytesRingBufferStats> onRingBufferStats() {
        return this.onRingBufferStats;
    }

    /**
     * Returns the path set for this queue.
     *
     * @return the file representing the path of the queue
     */
    @NotNull
    public File path() {
        return this.path;
    }

    /**
     * Sets the block size for memory-mapped files used by the queue.
     * The block size must be at least {@link #SMALL_BLOCK_SIZE}.
     *
     * @param blockSize the block size in bytes
     * @return the current builder instance for method chaining
     */
    public SingleChronicleQueueBuilder blockSize(long blockSize) {
        this.blockSize = Math.max(SMALL_BLOCK_SIZE, blockSize);
        return this;
    }

    /**
     * Overloaded method to set the block size using an integer value.
     *
     * @param blockSize the block size in bytes
     * @return the current builder instance for method chaining
     */
    public SingleChronicleQueueBuilder blockSize(int blockSize) {
        return blockSize((long) blockSize);
    }

    /**
     * @return - this is the size of a memory mapping chunk, a queue is read/written by using a number of blocks, you should avoid changing this unnecessarily.
     */
    public long blockSize() {

        long bs = blockSize == null
                ? DEFAULT_BLOCK_SIZE
                : blockSize;

        // can add an index2index & an index in one go.
        long minSize = Math.max(SMALL_BLOCK_SIZE, 32L * indexCount());
        return Math.max(minSize, bs);
    }

    /**
     * THIS IS FOR TESTING ONLY.
     * This makes the block size small to speed up short tests and show up issues which occur when moving from one block to another.
     * <p>
     * Using this will be slower when you have many messages, and break when you have large messages.
     * <p>
     * NOTE: This size is differnt on Linux and Windows. If you want the same size for both use {@code blockSize(OS.SAFE_PAGE_SIZE)}
     *
     * @return this
     */
    public SingleChronicleQueueBuilder testBlockSize() {
        // small size for testing purposes only.
        return blockSize(SMALL_BLOCK_SIZE);
    }

    /**
     * Sets the wire type to be used by the queue.
     *
     * @param wireType the wire type for serialization
     * @return the current builder instance for method chaining
     */
    @NotNull
    public SingleChronicleQueueBuilder wireType(@NotNull WireType wireType) {
        this.wireType = wireType;
        return this;
    }

    /**
     * Returns the wire type set for the queue. If not explicitly set, defaults to {@link WireType#BINARY_LIGHT}.
     *
     * @return the wire type used by the queue
     */
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
        return epoch == null ? Jvm.getLong(QueueSystemProperties.DEFAULT_EPOCH_PROPERTY, 0L) : epoch;
    }

    /**
     * @return BufferMode to use for writes. Only None is available is the OSS
     */
    @NotNull
    public BufferMode writeBufferMode() {
        return writeBufferMode == null
                ? BufferMode.None : writeBufferMode;
    }

    /**
     * When writeBufferMode is set to {@code Asynchronous}, uses a ring buffer to buffer appends, excerpts are written to the
     * Chronicle Queue using a background thread.
     * See also {@link #bufferCapacity()}
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
     * Returns the event loop set for the queue. If no event loop has been set, it creates
     * and returns a new {@link OnDemandEventLoop} with a {@link MediumEventLoop}.
     *
     * @return the current event loop or a new event loop instance if none has been set
     */
    @NotNull
    public EventLoop eventLoop() {
        if (eventLoop == null)
            return new OnDemandEventLoop(
                    () -> new MediumEventLoop(null, path.getName(), Pauser.busy(), true, "none"));
        return eventLoop;
    }

    /**
     * Sets the event loop to be used by the queue.
     *
     * @param eventLoop the event loop to set
     * @return the current builder instance for method chaining
     */
    @NotNull
    public SingleChronicleQueueBuilder eventLoop(EventLoop eventLoop) {
        this.eventLoop = eventLoop;
        return this;
    }

    /**
     * Checks if ring buffer monitoring is enabled. This feature is not available in the open-source version (OSS).
     *
     * @return true if ring buffer monitoring is enabled, false otherwise
     */
    public boolean enableRingBufferMonitoring() {
        return enableRingBufferMonitoring != null && enableRingBufferMonitoring;
    }

    /**
     * Enables or disables the ring buffer monitoring feature.
     *
     * @param enableRingBufferMonitoring true to enable monitoring, false to disable it
     * @return the current builder instance for method chaining
     */
    public SingleChronicleQueueBuilder enableRingBufferMonitoring(boolean enableRingBufferMonitoring) {
        this.enableRingBufferMonitoring = enableRingBufferMonitoring;
        return this;
    }

    /**
     * default value is {@code false} since 5.21ea0
     *
     * @return if ring buffer reader processes can invoke the CQ drainer, otherwise only writer processes can
     */
    public boolean ringBufferReaderCanDrain() {
        return ringBufferReaderCanDrain != null && ringBufferReaderCanDrain;
    }

    /**
     * Sets whether ring buffer reader processes are allowed to invoke the Chronicle Queue drainer.
     *
     * @param ringBufferReaderCanDrain true to allow reader processes to invoke the drainer, false to restrict it
     * @return the current builder instance for method chaining
     */
    public SingleChronicleQueueBuilder ringBufferReaderCanDrain(boolean ringBufferReaderCanDrain) {
        this.ringBufferReaderCanDrain = ringBufferReaderCanDrain;
        return this;
    }

    /**
     * @return whether to force creating a reader (to recover from crash)
     */
    public boolean ringBufferForceCreateReader() {
        return ringBufferForceCreateReader != null && ringBufferForceCreateReader;
    }

    /**
     * Sets whether the queue should force creating a ring buffer reader to recover from crashes.
     *
     * @param ringBufferForceCreateReader true to force reader creation, false otherwise
     * @return the current builder instance for method chaining
     */
    public SingleChronicleQueueBuilder ringBufferForceCreateReader(boolean ringBufferForceCreateReader) {
        this.ringBufferForceCreateReader = ringBufferForceCreateReader;
        return this;
    }

    /**
     * @return if ring buffer readers are not reset on close. If true then re-opening a reader puts you back
     * at the same place. If true, your reader can block writers if the reader is not open
     */
    public boolean ringBufferReopenReader() {
        return ringBufferReopenReader != null && ringBufferReopenReader;
    }

    /**
     * Sets whether the ring buffer readers should reopen at the same position upon closing.
     *
     * @param ringBufferReopenReader true to reopen readers at the same position, false otherwise
     * @return the current builder instance for method chaining
     */
    public SingleChronicleQueueBuilder ringBufferReopenReader(boolean ringBufferReopenReader) {
        this.ringBufferReopenReader = ringBufferReopenReader;
        return this;
    }

    /**
     * Returns the priority of the handler for the async mode drainer.
     *
     * @return the priority for the drainer handler
     */
    public HandlerPriority drainerPriority() {
        return drainerPriority;
    }

    /**
     * Sets the priority of the handler for the async mode drainer.
     *
     * @param drainerPriority the priority to set for the drainer handler
     * @return the current builder instance for method chaining
     */
    public SingleChronicleQueueBuilder drainerPriority(HandlerPriority drainerPriority) {
        this.drainerPriority = drainerPriority;
        return this;
    }

    /**
     * Returns the timeout for the drainer in milliseconds. If the timeout is not set or is less than or equal to 0,
     * the default value of 10,000 milliseconds (10 seconds) is returned.
     *
     * @return the drainer timeout in milliseconds
     */
    public int drainerTimeoutMS() {
        return drainerTimeoutMS <= 0 ? 10_000 : drainerTimeoutMS;
    }

    /**
     * Sets the timeout for the drainer in milliseconds.
     *
     * @param timeout the timeout in milliseconds
     * @return the current builder instance for method chaining
     */
    public SingleChronicleQueueBuilder drainerTimeoutMS(int timeout) {
        drainerTimeoutMS = timeout;
        return this;
    }

    /**
     * Returns the {@link Pauser} supplier to be used by the ring buffer when waiting.
     * If no supplier is set, it defaults to a busy-wait {@link Pauser}.
     *
     * @return the supplier of {@link Pauser} for the ring buffer
     */
    public Supplier<Pauser> ringBufferPauserSupplier() {
        return ringBufferPauserSupplier == null ? Pauser::busy : ringBufferPauserSupplier;
    }

    /**
     * Sets the {@link Pauser} supplier to be used by the ring buffer when waiting.
     *
     * @param ringBufferPauserSupplier the supplier of {@link Pauser} for the ring buffer
     * @return the current builder instance for method chaining
     */
    public SingleChronicleQueueBuilder ringBufferPauserSupplier(Supplier<Pauser> ringBufferPauserSupplier) {
        this.ringBufferPauserSupplier = ringBufferPauserSupplier;
        return this;
    }

    /**
     * Sets the number of indices to be maintained in the queue's index. The number is rounded up to the next power of 2,
     * with a minimum value of 8.
     *
     * @param indexCount the number of indices to set
     * @return the current builder instance for method chaining
     */
    public SingleChronicleQueueBuilder indexCount(int indexCount) {
        this.indexCount = Maths.nextPower2(indexCount, 8);
        return this;
    }

    /**
     * Returns the number of indices for the queue's index. If no index count is explicitly set,
     * it defaults to the roll cycle's {@link RollCycle#defaultIndexCount()}.
     *
     * @return the number of indices for the queue
     */
    public int indexCount() {
        return indexCount == null || indexCount <= 0 ? rollCycle().defaultIndexCount() : indexCount;
    }

    /**
     * Sets the index spacing, ensuring the value is rounded up to the next power of 2.
     *
     * @param indexSpacing the index spacing to set
     * @return the current builder instance for method chaining
     */
    public SingleChronicleQueueBuilder indexSpacing(int indexSpacing) {
        this.indexSpacing = Maths.nextPower2(indexSpacing, 1);
        return this;
    }

    /**
     * Returns the index spacing. If no spacing is explicitly set, it defaults to the roll cycle's
     * {@link RollCycle#defaultIndexSpacing()}.
     *
     * @return the index spacing for the queue
     */
    public int indexSpacing() {
        return indexSpacing == null || indexSpacing <= 0 ? rollCycle().defaultIndexSpacing() :
                indexSpacing;
    }

    /**
     * Returns the {@link TimeProvider} for the queue. If not explicitly set, it defaults to the
     * {@link SystemTimeProvider#INSTANCE}.
     *
     * @return the time provider used by the queue
     */
    public TimeProvider timeProvider() {
        return timeProvider == null ? SystemTimeProvider.INSTANCE : timeProvider;
    }

    /**
     * Sets the {@link TimeProvider} for the queue.
     *
     * @param timeProvider the time provider to set
     * @return the current builder instance for method chaining
     */
    public SingleChronicleQueueBuilder timeProvider(TimeProvider timeProvider) {
        this.timeProvider = timeProvider;
        return this;
    }

    /**
     * Returns the {@link TimingPauser} supplier used by the queue.
     * If not explicitly set, it defaults to {@link DefaultPauserSupplier#INSTANCE}.
     *
     * @return the pauser supplier for the queue
     */
    public Supplier<TimingPauser> pauserSupplier() {
        return pauserSupplier == null ? TIMING_PAUSER_SUPPLIER : pauserSupplier;
    }

    /**
     * Sets the {@link TimingPauser} supplier for the queue.
     *
     * @param pauser the pauser supplier to set
     * @return the current builder instance for method chaining
     */
    public SingleChronicleQueueBuilder pauserSupplier(Supplier<TimingPauser> pauser) {
        this.pauserSupplier = pauser;
        return this;
    }

    /**
     * Sets the timeout in milliseconds for the queue.
     *
     * @param timeoutMS the timeout in milliseconds
     * @return the current builder instance for method chaining
     */
    public SingleChronicleQueueBuilder timeoutMS(long timeoutMS) {
        this.timeoutMS = timeoutMS;
        return this;
    }

    /**
     * Returns the timeout in milliseconds for the queue. If not set, defaults to 10,000 milliseconds (10 seconds).
     *
     * @return the timeout in milliseconds
     */
    public long timeoutMS() {
        return timeoutMS == null ? 10_000L : timeoutMS;
    }

    /**
     * Sets the {@link StoreFileListener} for the queue.
     *
     * @param storeFileListener the store file listener to set
     * @return the current builder instance for method chaining
     */
    public SingleChronicleQueueBuilder storeFileListener(StoreFileListener storeFileListener) {
        this.storeFileListener = storeFileListener;
        return this;
    }

    /**
     * Returns the {@link StoreFileListener} used by the queue. If not explicitly set,
     * it defaults to {@link StoreFileListeners#DEBUG}.
     *
     * @return the store file listener for the queue
     */
    @NotNull
    public StoreFileListener storeFileListener() {
        if (storeFileListener != null)
            return storeFileListener;
        return DEBUG_FILE_RELEASED ? StoreFileListeners.DEBUG : StoreFileListeners.NO_OP;
    }

    /**
     * Sets the source ID for the queue. The source ID must be a positive integer.
     *
     * @param sourceId the source ID to set
     * @return the current builder instance for method chaining
     * @throws IllegalArgumentException if the source ID is negative
     */
    public SingleChronicleQueueBuilder sourceId(int sourceId) {
        if (sourceId < 0)
            throw new IllegalArgumentException("Invalid source Id, must be positive");
        this.sourceId = sourceId;
        return this;
    }

    /**
     * Returns the source ID for the queue. If not explicitly set, defaults to 0.
     *
     * @return the source ID for the queue
     */
    public int sourceId() {
        return sourceId == null ? 0 : sourceId;
    }

    /**
     * Checks if the queue is in read-only mode. On Windows, read-only mode is not supported and
     * the queue defaults to read/write mode.
     *
     * @return true if the queue is in read-only mode, false otherwise
     */
    public boolean readOnly() {
        return Boolean.TRUE.equals(readOnly) && !OS.isWindows();
    }

    /**
     * Sets whether the queue is in read-only mode. If the platform is Windows, it logs a warning
     * and defaults to read/write mode as read-only mode is not supported on Windows.
     *
     * @param readOnly true to enable read-only mode, false otherwise
     * @return the current builder instance for method chaining
     */
    public SingleChronicleQueueBuilder readOnly(boolean readOnly) {
        if (OS.isWindows() && readOnly)
            Jvm.warn().on(SingleChronicleQueueBuilder.class,
                    "Read-only mode is not supported on Windows® platforms, defaulting to read/write.");
        else
            this.readOnly = readOnly;

        return this;
    }

    /**
     * Checks if double-buffering is enabled for the queue. Double-buffering allows writing
     * without waiting for the write lock, reducing the cost of serialization on contention.
     *
     * @return true if double-buffering is enabled, false otherwise
     */
    public boolean doubleBuffer() {
        return doubleBuffer;
    }

    /**
     * <p>
     * Enables double-buffered writes on contention.
     * <p>
     * Normally, all writes to the queue will be serialized based on the write lock acquisition. Each time {@link ExcerptAppender#writingDocument()}
     * is called, appender tries to acquire the write lock on the queue, and if it fails to do so it blocks until write
     * lock is unlocked, and in turn locks the queue for itself.
     * <p>
     * When double-buffering is enabled, if appender sees that the write lock is acquired upon {@link ExcerptAppender#writingDocument()} call,
     * it returns immediately with a context pointing to the secondary buffer, and essentially defers lock acquisition
     * until the context.close() is called (normally with try-with-resources pattern it is at the end of the try block),
     * allowing user to go ahead writing data, and then essentially doing memcpy on the serialized data (thus reducing cost of serialization).
     * <p>
     * This is only useful if (majority of) the objects being written to the queue are big enough AND their marshalling is not straight-forward
     * (e.g. BytesMarshallable's marshalling is very efficient and quick and hence double-buffering will only slow things down), and if there's a
     * heavy contention on writes (e.g. 2 or more threads writing a lot of data to the queue at a very high rate).
     */
    public SingleChronicleQueueBuilder doubleBuffer(boolean doubleBuffer) {
        this.doubleBuffer = doubleBuffer;
        return this;
    }

    /**
     * Returns the encoding supplier used by the queue, if set. The encoding supplier is responsible
     * for encoding data written to the queue.
     *
     * @return the encoding supplier, or null if not set
     */
    public Supplier<BiConsumer<BytesStore<?, ?>, Bytes<?>>> encodingSupplier() {
        return encodingSupplier;
    }

    /**
     * Returns the decoding supplier used by the queue, if set. The decoding supplier is responsible
     * for decoding data read from the queue.
     *
     * @return the decoding supplier, or null if not set
     */
    public Supplier<BiConsumer<BytesStore<?, ?>, Bytes<?>>> decodingSupplier() {
        return decodingSupplier;
    }

    /**
     * Sets both the encoding and decoding suppliers for the queue. Both suppliers must be set together;
     * if one is set to null, the other must also be null.
     *
     * @param encodingSupplier the encoding supplier for writing data
     * @param decodingSupplier the decoding supplier for reading data
     * @return the current builder instance for method chaining
     * @throws UnsupportedOperationException if one supplier is set and the other is null
     */
    public SingleChronicleQueueBuilder codingSuppliers(@Nullable
                                                       Supplier<BiConsumer<BytesStore<?, ?>, Bytes<?>>> encodingSupplier,
                                                       @Nullable Supplier<BiConsumer<BytesStore<?, ?>, Bytes<?>>> decodingSupplier) {
        if ((encodingSupplier == null) != (decodingSupplier == null))
            throw new UnsupportedOperationException("Both encodingSupplier and decodingSupplier must be set or neither");
        this.encodingSupplier = encodingSupplier;
        this.decodingSupplier = decodingSupplier;
        return this;
    }

    /**
     * Returns the {@link SecretKeySpec} used for AES encryption, if set.
     *
     * @return the encryption key, or null if no key is set
     */
    public SecretKeySpec key() {
        return key;
    }

    /**
     * Pre-build method that initializes metadata before constructing the queue.
     * If an error occurs during initialization, the metadata store is closed and the exception is rethrown.
     * It also resets the roll time if both {@code rollTime} and {@code rollTimeZone} are provided and the epoch is unset.
     */
    protected void preBuild() {
        try {
            initializeMetadata();
        } catch (Exception ex) {
            Closeable.closeQuietly(metaStore);
            throw ex;
        }
        if ((epoch == null || epoch == 0) && (rollTime != null && rollTimeZone != null))
            // Reset roll time if epoch is unset but rollTime and rollTimeZone are provided
            rollTime(rollTime, rollTimeZone);
    }

    /**
     * Checks whether interrupts should be monitored, based on system properties or the configuration.
     *
     * @return true if interrupts should be checked, false otherwise
     */
    public boolean checkInterrupts() {

        if (System.getProperties().contains("chronicle.queue.checkInterrupts"))
            return Jvm.getBoolean("chronicle.queue.checkInterrupts");

        return checkInterrupts;
    }

    /**
     * Sets whether the queue should monitor interrupts.
     *
     * @param checkInterrupts true to enable interrupt checking, false otherwise
     * @return the current builder instance for method chaining
     */
    public SingleChronicleQueueBuilder checkInterrupts(boolean checkInterrupts) {
        this.checkInterrupts = checkInterrupts;
        return this;
    }

    /**
     * Returns the interval in milliseconds for forcing a directory listing refresh.
     *
     * @return the refresh interval in milliseconds
     */
    public long forceDirectoryListingRefreshIntervalMs() {
        return forceDirectoryListingRefreshIntervalMs;
    }

    /**
     * Sets the interval in milliseconds for forcing a directory listing refresh.
     *
     * @param forceDirectoryListingRefreshIntervalMs the interval to set
     * @return the current builder instance for method chaining
     */
    public SingleChronicleQueueBuilder forceDirectoryListingRefreshIntervalMs(long forceDirectoryListingRefreshIntervalMs) {
        this.forceDirectoryListingRefreshIntervalMs = forceDirectoryListingRefreshIntervalMs;
        return this;
    }

    /**
     * WARNING: Avoid using this method as it can have unintended consequences.
     * We plan to phase it out.
     * It's only a shallow copy so field will have the same objects.
     */
    @SuppressWarnings("java:S2975")
    public SingleChronicleQueueBuilder clone() {
        try {
            return (SingleChronicleQueueBuilder) super.clone();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Copies all null fields in the current builder from the given parent builder.
     * Only fields present in both builders' class hierarchies will be copied.
     *
     * @param parentBuilder the parent builder to copy from
     * @return the current builder instance for method chaining
     * @throws IllegalArgumentException if the builders are not from the same class hierarchy
     */

    public SingleChronicleQueueBuilder setAllNullFields(@Nullable SingleChronicleQueueBuilder parentBuilder) {
        if (parentBuilder == null)
            return this;

        // Ensure both builders are from the same class hierarchy
        if (!(this.getClass().isAssignableFrom(parentBuilder.getClass()) || parentBuilder.getClass().isAssignableFrom(this.getClass())))
            throw new IllegalArgumentException("Classes are not in same implementation hierarchy");

        // Get field information from both builders
        List<FieldInfo> sourceFieldInfo = Wires.fieldInfos(parentBuilder.getClass());

        // Copy null fields from the parentBuilder
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

    /**
     * Returns the append lock used for appending to the queue. If the queue is read-only,
     * it returns a no-op lock; otherwise, it returns a standard {@link AppendLock}.
     *
     * @return the append lock for the queue
     */
    public WriteLock appendLock() {
        return readOnly() ? WriteLock.NO_OP : new AppendLock(metaStore, pauserSupplier(), timeoutMS() * 3 / 2);
    }

    /**
     * Sets an {@link AppenderListener} to be called when an excerpt is written.
     * This listener is invoked while the write lock is still held, after the message has been written.
     * In asynchronous writes, it is called in the background thread.
     *
     * @param appenderListener the listener to call when an excerpt is written
     * @return the current builder instance for method chaining
     */
    public SingleChronicleQueueBuilder appenderListener(AppenderListener appenderListener) {
        this.appenderListener = appenderListener;
        return this;
    }

    /**
     * Returns the {@link AppenderListener} currently set for the queue.
     *
     * @return the appender listener
     */
    public AppenderListener appenderListener() {
        return appenderListener;
    }

    /**
     * Sets the synchronization mode for the queue's memory-mapped files.
     *
     * @param syncMode the sync mode to set
     * @return the current builder instance for method chaining
     */
    public SingleChronicleQueueBuilder syncMode(SyncMode syncMode) {
        this.syncMode = syncMode;
        return this;
    }

    /**
     * Returns the synchronization mode used for the queue's memory-mapped files.
     * If not explicitly set, it defaults to the {@link MappedFile#DEFAULT_SYNC_MODE}.
     *
     * @return the sync mode for the queue
     */
    public SyncMode syncMode() {
        return syncMode == null ? MappedFile.DEFAULT_SYNC_MODE : syncMode;
    }

    /**
     * A default supplier for the {@link TimingPauser}, used when no explicit supplier is provided.
     * This implementation returns a {@link YieldingPauser} with a 500,000 nanosecond yield duration.
     */
    enum DefaultPauserSupplier implements Supplier<TimingPauser> {
        INSTANCE;

        @Override
        public TimingPauser get() {
            return new YieldingPauser(500_000);
        }
    }
}
