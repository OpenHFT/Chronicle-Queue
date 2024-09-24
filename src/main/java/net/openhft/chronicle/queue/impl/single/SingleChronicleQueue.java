/*
 * Copyright 2016-2020 chronicle.software
 *
 *       https://chronicle.software
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

import net.openhft.chronicle.bytes.*;
import net.openhft.chronicle.bytes.internal.HeapBytesStore;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.analytics.AnalyticsFacade;
import net.openhft.chronicle.core.annotation.PackageLocal;
import net.openhft.chronicle.core.announcer.Announcer;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.scoped.ScopedResource;
import net.openhft.chronicle.core.threads.CleaningThreadLocal;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.InterruptedRuntimeException;
import net.openhft.chronicle.core.threads.OnDemandEventLoop;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.core.util.StringUtils;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.*;
import net.openhft.chronicle.queue.impl.single.namedtailer.IndexUpdater;
import net.openhft.chronicle.queue.impl.single.namedtailer.IndexUpdaterFactory;
import net.openhft.chronicle.queue.impl.table.SingleTableStore;
import net.openhft.chronicle.queue.internal.AnalyticsHolder;
import net.openhft.chronicle.threads.DiskSpaceMonitor;
import net.openhft.chronicle.threads.TimingPauser;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.security.SecureRandom;
import java.text.ParseException;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.function.*;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static net.openhft.chronicle.core.io.Closeable.closeQuietly;
import static net.openhft.chronicle.queue.TailerDirection.BACKWARD;
import static net.openhft.chronicle.queue.TailerDirection.NONE;
import static net.openhft.chronicle.wire.Wires.SPB_HEADER_SIZE;
import static net.openhft.chronicle.wire.Wires.acquireBytesScoped;

/**
 * SingleChronicleQueue is an implementation of RollingChronicleQueue that supports appending
 * and reading of data from a file-based queue with roll cycles. This class is responsible
 * for managing the lifecycle, rolling logic, and the underlying storage.
 *
 * It also supports various configurations such as event loop handling, wire types, buffer
 * management, and replication.
 */
@SuppressWarnings("this-escape")
public class SingleChronicleQueue extends AbstractCloseable implements RollingChronicleQueue {

    // Constants for file extensions, metadata files, and disk space monitoring
    public static final String SUFFIX = ".cq4";
    public static final String DISCARD_FILE_SUFFIX = ".discard";
    public static final String QUEUE_METADATA_FILE = "metadata" + SingleTableStore.SUFFIX;
    public static final String DISK_SPACE_CHECKER_NAME = DiskSpaceMonitor.DISK_SPACE_CHECKER_NAME;
    public static final String REPLICATED_NAMED_TAILER_PREFIX = "replicated:";
    public static final String INDEX_LOCK_FORMAT = "index.%s.lock";
    public static final String INDEX_VERSION_FORMAT = "index.%s.version";

    // Flag to check if cycle rolling needs to be enforced
    private static final boolean SHOULD_CHECK_CYCLE = Jvm.getBoolean("chronicle.queue.checkrollcycle");

    // Time (in ms) to warn for slow appenders
    static final int WARN_SLOW_APPENDER_MS = Jvm.getInteger("chronicle.queue.warnSlowAppenderMs", 100);

    @NotNull
    protected final EventLoop eventLoop; // The event loop used for managing background tasks
    @NotNull
    protected final TableStore<SCQMeta> metaStore; // Metadata store for queue settings
    @NotNull
    protected final WireStorePool pool; // Pool for managing WireStore instances
    protected final boolean doubleBuffer; // Flag to enable double-buffering for performance
    final Supplier<TimingPauser> pauserSupplier; // Supplier for TimingPauser instances
    final long timeoutMS; // Timeout in milliseconds for operations
    @NotNull
    final File path; // The path to the queue's directory
    final String fileAbsolutePath; // Absolute path of the queue file

    // MetaStoreMap stores BytesStore as keys and LongValue as values, synchronized for write
    @SuppressWarnings("rawtypes")
    private final Map<BytesStore, LongValue> metaStoreMap = new ConcurrentHashMap<>();

    // StoreSupplier provides a way to create store instances
    private final StoreSupplier storeSupplier;

    // The epoch time used for calculating roll cycles
    private final long epoch;

    // Flag to indicate whether buffering is enabled
    private final boolean isBuffered;

    @NotNull
    private final WireType wireType; // The type of wire (binary, text, etc.)

    private final long blockSize; // The size of each block in the queue
    private final long overlapSize; // The size of the overlap between blocks
    @NotNull
    private final Consumer<BytesRingBufferStats> onRingBufferStats; // Callback for ring buffer statistics
    private final long bufferCapacity; // Capacity of the buffer used
    private final int indexSpacing; // Spacing between index entries
    private final int indexCount; // Number of index entries
    @NotNull
    private final TimeProvider time; // Time provider for the queue
    @NotNull
    private final BiFunction<RollingChronicleQueue, Wire, SingleChronicleQueueStore> storeFactory; // Factory for creating stores
    private final Set<Closeable> closers = Collections.newSetFromMap(new IdentityHashMap<>()); // Set of closeable resources
    private final boolean readOnly; // Flag indicating if the queue is read-only
    @NotNull
    private final CycleCalculator cycleCalculator; // Cycle calculator for rolling logic
    @Nullable
    private final LongValue lastAcknowledgedIndexReplicated; // Last acknowledged replicated index
    @Nullable
    private final LongValue lastIndexReplicated; // Last replicated index
    private final LongValue lastIndexMSynced; // Last index synced in milliseconds
    @NotNull
    private final DirectoryListing directoryListing; // Listing of directory contents for the queue
    @NotNull
    private final WriteLock writeLock; // Write lock for synchronization
    private final boolean checkInterrupts; // Flag to enable interrupt checking
    @NotNull
    private final RollingResourcesCache dateCache; // Cache for rolled resources by date
    private final WriteLock appendLock; // Lock used for appending operations
    private final StoreFileListener storeFileListener; // Listener for store file events
    @NotNull
    private final RollCycle rollCycle; // The roll cycle for the queue
    private final int deltaCheckpointInterval; // Interval for delta checkpointing
    final AppenderListener appenderListener; // Listener for appender events
    protected int sourceId; // ID representing the source of the queue
    private int cycleFileRenamed = -1; // Tracks renamed cycle files
    @NotNull
    private Condition createAppenderCondition = NoOpCondition.INSTANCE; // Condition used for appender creation
    protected final ThreadLocal<ExcerptAppender> strongExcerptAppenderThreadLocal = CleaningThreadLocal.withCloseQuietly(this::createNewAppenderOnceConditionIsMet);
    private final long forceDirectoryListingRefreshIntervalMs; // Interval for refreshing directory listing
    private final long[] chunkCount = {0}; // Tracks the count of chunks
    private final SyncMode syncMode; // Synchronization mode for the queue


    /**
     * Constructs a SingleChronicleQueue with the specified builder configuration.
     * This constructor sets up various configurations like rolling cycle, epoch,
     * buffering, path, wire type, and other queue-related settings based on the builder.
     *
     * @param builder the SingleChronicleQueueBuilder containing the configuration
     */
    protected SingleChronicleQueue(@NotNull final SingleChronicleQueueBuilder builder) {
        try {
            // Set the roll cycle and epoch time for the queue
            rollCycle = builder.rollCycle();
            cycleCalculator = cycleCalculator(builder.rollTimeZone());
            long epoch0 = builder.epoch();
            epoch = epoch0 == 0 ? rollCycle.defaultEpoch() : epoch0;

            // Initialize the rolling resources cache based on the roll cycle and path
            dateCache = new RollingResourcesCache(rollCycle, epoch, textToFile(builder), fileToText());

            // Setup the store file listener and supplier for wire stores
            storeFileListener = builder.storeFileListener();
            storeSupplier = new StoreSupplier();
            pool = WireStorePool.withSupplier(storeSupplier, storeFileListener);

            // Set whether the queue uses asynchronous buffering
            isBuffered = BufferMode.Asynchronous == builder.writeBufferMode();

            // Set the path and ensure the directory exists
            path = builder.path();
            if (!builder.readOnly())
                //noinspection ResultOfMethodCallIgnored
                path.mkdirs(); // Create directories if they do not exist

            fileAbsolutePath = path.getAbsolutePath(); // Get absolute file path of the queue directory

            wireType = builder.wireType(); // Set the wire type (binary, text, etc.)
            blockSize = builder.blockSize(); // Set the block size for the queue

            // Calculate overlap size based on the block size
            overlapSize = calcOverlapSize(blockSize);

            eventLoop = builder.eventLoop(); // Initialize the event loop for managing background tasks
            bufferCapacity = builder.bufferCapacity(); // Set buffer capacity
            onRingBufferStats = builder.onRingBufferStats(); // Set ring buffer stats consumer
            indexCount = builder.indexCount(); // Set the number of index entries
            indexSpacing = builder.indexSpacing(); // Set the spacing between index entries
            time = builder.timeProvider(); // Set the time provider for the queue
            pauserSupplier = builder.pauserSupplier(); // Set supplier for timing pauser

            // Set timeout with a random 20% element to avoid simultaneous timeouts across threads
            timeoutMS = (long) (builder.timeoutMS() * (1 + 0.2 * new SecureRandom().nextFloat()));

            storeFactory = builder.storeFactory(); // Factory for creating queue stores
            checkInterrupts = builder.checkInterrupts(); // Check for interrupts during processing
            metaStore = builder.metaStore(); // Set the metadata store for the queue
            doubleBuffer = builder.doubleBuffer(); // Enable double buffering if required
            syncMode = builder.syncMode(); // Set synchronization mode

            // Handle read-only mode for metaStore
            if (metaStore.readOnly() && !builder.readOnly()) {
                Jvm.warn().on(getClass(), "Forcing queue to be readOnly file=" + path);
                builder.readOnly(metaStore.readOnly()); // Force read-only mode if metadata is read-only
            }

            readOnly = builder.readOnly(); // Set read-only mode for the queue
            appenderListener = builder.appenderListener(); // Set appender listener

            // Initialize directory listing based on read-only mode
            if (metaStore.readOnly()) {
                this.directoryListing = new FileSystemDirectoryListing(path, fileNameToCycleFunction());
            } else {
                this.directoryListing = readOnly ? new TableDirectoryListingReadOnly(metaStore)
                                                 : new TableDirectoryListing(metaStore, path.toPath(), fileNameToCycleFunction());
                directoryListing.init(); // Initialize the directory listing
            }

            this.directoryListing.refresh(true); // Refresh directory listing
            this.writeLock = builder.writeLock(); // Set the write lock for synchronization

            // Release write lock if the process is dead (applies to TableStoreWriteLock)
            if (writeLock instanceof TableStoreWriteLock) {
                writeLock.forceUnlockIfProcessIsDead();
            }

            this.appendLock = builder.appendLock(); // Set the append lock

            // Handle replicated indices based on read-only mode
            if (readOnly) {
                this.lastIndexReplicated = null;
                this.lastAcknowledgedIndexReplicated = null;
                this.lastIndexMSynced = null;
            } else {
                this.lastIndexReplicated = metaStore.doWithExclusiveLock(ts -> ts.acquireValueFor("chronicle.lastIndexReplicated", -1L));
                this.lastAcknowledgedIndexReplicated = metaStore.doWithExclusiveLock(ts -> ts.acquireValueFor("chronicle.lastAcknowledgedIndexReplicated", -1L));
                this.lastIndexMSynced = metaStore.doWithExclusiveLock(ts -> ts.acquireValueFor("chronicle.lastIndexMSynced", -1L));
            }

            this.deltaCheckpointInterval = builder.deltaCheckpointInterval(); // Set delta checkpoint interval
            this.forceDirectoryListingRefreshIntervalMs = builder.forceDirectoryListingRefreshIntervalMs(); // Set directory listing refresh interval

            sourceId = builder.sourceId(); // Set the source ID for the queue

            // Monitor disk space usage for the queue's path
            DiskSpaceMonitor.INSTANCE.pollDiskSpace(path);

            // Announce the Chronicle Queue usage and analytics details
            Announcer.announce("net.openhft", "chronicle-queue",
                    AnalyticsFacade.isEnabled()
                            ? singletonMap("Analytics", "Chronicle Queue reports usage statistics. Learn more or turn off: https://github.com/OpenHFT/Chronicle-Queue/blob/ea/DISCLAIMER.adoc")
                            : emptyMap());

            // Send analytics event if enabled
            final Map<String, String> additionalEventParameters = AnalyticsFacade.standardAdditionalProperties();
            additionalEventParameters.put("wire_type", wireType.toString());
            final String rollCycleName = rollCycle.toString();
            if (!rollCycleName.startsWith("TEST"))
                additionalEventParameters.put("roll_cycle", rollCycleName);

            AnalyticsHolder.instance().sendEvent("started", additionalEventParameters);

            singleThreadedCheckDisabled(true); // Disable single-threaded check
        } catch (Throwable t) {
            close(); // Close the queue if an exception occurs
            throw Jvm.rethrow(t);
        }
    }

    /**
     * Calculates the overlap size based on the block size.
     * Ensures that the overlap size is capped at 1GB (1L << 30).
     *
     * @param blockSize the block size for the queue
     * @return the calculated overlap size
     */
    private static long calcOverlapSize(long blockSize) {
        final long overlapSize;
        if (blockSize < OS.SAFE_PAGE_SIZE)
            overlapSize = blockSize;
        else if (blockSize < OS.SAFE_PAGE_SIZE * 4)
            overlapSize = OS.SAFE_PAGE_SIZE;
        else if (blockSize < 4L << 30)
            overlapSize = blockSize / 4;
        else
            overlapSize = 1L << 30; // Maximum overlap size is 1GB
        return overlapSize;
    }

    /**
     * Sets a custom condition to be used for appender creation.
     *
     * @param createAppenderCondition the condition to be used for appender creation
     */
    protected void createAppenderCondition(@NotNull Condition createAppenderCondition) {
        this.createAppenderCondition = createAppenderCondition;
    }

    /**
     * Returns the default cycle calculator. The cycle calculator is responsible
     * for determining the rolling intervals and cycles based on the time zone.
     *
     * @param zoneId the ZoneId used for cycle calculation
     * @return the CycleCalculator instance
     */
    protected CycleCalculator cycleCalculator(ZoneId zoneId) {
        return DefaultCycleCalculator.INSTANCE;
    }

    /**
     * Converts a text string into a file using the queue's path and suffix.
     *
     * @param builder the SingleChronicleQueueBuilder containing the path configuration
     * @return a Function that converts text to a File
     */
    @NotNull
    private Function<String, File> textToFile(@NotNull SingleChronicleQueueBuilder builder) {
        return name -> new File(builder.path(), name + SUFFIX);
    }

    /**
     * Converts a File object into a text string, stripping the queue file suffix.
     *
     * @return a Function that converts a File to its name as a String
     */
    @NotNull
    private Function<File, String> fileToText() {
        return file -> {
            String name = file.getName();
            return name.substring(0, name.length() - SUFFIX.length());
        };
    }

    /**
     * Returns the source ID of this queue.
     *
     * @return the source ID as an integer
     */
    @Override
    public int sourceId() {
        return sourceId;
    }

    /**
     * Returns the highest last index that has been confirmed to be read by all remote hosts during replication.
     * If replication is not enabled, returns -1.
     *
     * @return the last acknowledged replicated index or -1 if not available
     */
    @Override
    public long lastAcknowledgedIndexReplicated() {
        return lastAcknowledgedIndexReplicated == null ? -1 : lastAcknowledgedIndexReplicated.getVolatileValue(-1);
    }

    /**
     * Updates the last acknowledged index that has been replicated to all remote hosts.
     *
     * @param newValue the new last acknowledged index value
     */
    @Override
    public void lastAcknowledgedIndexReplicated(long newValue) {
        if (lastAcknowledgedIndexReplicated != null)
            lastAcknowledgedIndexReplicated.setMaxValue(newValue);
    }

    /**
     * Refreshes the directory listing, ensuring it is up-to-date.
     * Throws an exception if the queue has been closed.
     */
    @Override
    public void refreshDirectoryListing() {
        throwExceptionIfClosed();

        directoryListing.refresh(true);
    }

    /**
     * Returns the maximum last index that has been sent to any remote host during replication.
     * If replication is not enabled, returns -1.
     *
     * @return the last replicated index or -1 if not available
     */
    @Override
    public long lastIndexReplicated() {
        return lastIndexReplicated == null ? -1 : lastIndexReplicated.getVolatileValue(-1);
    }

    /**
     * Updates the last index that has been replicated to remote hosts.
     *
     * @param indexReplicated the new last replicated index value
     */
    @Override
    public void lastIndexReplicated(long indexReplicated) {
        if (lastIndexReplicated != null)
            lastIndexReplicated.setMaxValue(indexReplicated);
    }

    /**
     * Returns the last index that has been synchronized in milliseconds.
     * If synchronization is not enabled, returns -1.
     *
     * @return the last synchronized index in milliseconds, or -1 if not available
     */
    @Override
    public long lastIndexMSynced() {
        return lastIndexMSynced == null ? -1 : lastIndexMSynced.getVolatileValue(-1);
    }

    /**
     * Updates the last index that has been synchronized in milliseconds.
     *
     * @param lastIndexMSynced the new last synchronized index in milliseconds
     */
    @Override
    public void lastIndexMSynced(long lastIndexMSynced) {
        if (this.lastIndexMSynced != null)
            this.lastIndexMSynced.setMaxValue(lastIndexMSynced);
    }

    /**
     * Unsupported operation. Currently, clear is not implemented.
     */
    @Override
    public void clear() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Returns the file representing the queue's directory path.
     *
     * @return the File object for the queue's directory
     */
    @Override
    @NotNull
    public File file() {
        return path;
    }

    /**
     * Returns the absolute path of the queue's directory as a string.
     *
     * @return the absolute path of the queue directory
     */
    @NotNull
    @Override
    public String fileAbsolutePath() {
        return fileAbsolutePath;
    }

    /**
     * Dumps the last header of the last cycle of the queue. This provides debugging
     * information about the state of the last cycle's header.
     *
     * @return a string representation of the last cycle's header
     */
    @Override
    public @NotNull String dumpLastHeader() {
        StringBuilder sb = new StringBuilder(256);
        try (SingleChronicleQueueStore wireStore = storeForCycle(lastCycle(), epoch, false, null)) {
            sb.append(wireStore.dumpHeader());
        }
        return sb.toString();
    }

    /**
     * Dumps the contents of the entire queue, including metadata and each cycle.
     * This provides a full debugging view of the queue's state.
     *
     * @return a string representation of the queue's metadata and cycles
     */
    @NotNull
    @Override
    public String dump() {
        StringBuilder sb = new StringBuilder(1024);
        sb.append(metaStore.dump(wireType)); // Append metadata store dump
        for (int i = firstCycle(), max = lastCycle(); i <= max; i++) {
            try (SingleChronicleQueueStore commonStore = storeForCycle(i, epoch, false, null)) {
                if (commonStore != null)
                    sb.append(commonStore.dump(wireType)); // Append cycle dump
            }
        }
        return sb.toString();
    }

    /**
     * Dumps the contents of the queue from a given index range into a Writer.
     * If there are no more messages in the queue or the target index is beyond the range, it terminates the dump.
     *
     * @param writer   the Writer to output the queue contents to
     * @param fromIndex the starting index from where to dump
     * @param toIndex   the ending index where the dump should stop
     */
    @Override
    public void dump(@NotNull Writer writer, long fromIndex, long toIndex) {
        try {
            long firstIndex = firstIndex();
            writer.append("# firstIndex: ").append(Long.toHexString(firstIndex)).append("\n");

            // Create a tailer to read from the starting index
            try (ExcerptTailer tailer = createTailer()) {
                if (!tailer.moveToIndex(fromIndex)) {
                    // If the first index is greater than the fromIndex, start from the beginning
                    if (firstIndex > fromIndex) {
                        tailer.toStart();
                    } else {
                        return; // Return if the index is invalid
                    }
                }

                // Use a scoped resource for acquiring bytes
                try (ScopedResource<Bytes<Void>> stlBytes = acquireBytesScoped()) {
                    Bytes<?> bytes = stlBytes.get();
                    TextWire text = new TextWire(bytes);

                    // Iterate through documents and dump their contents
                    while (true) {
                        try (DocumentContext dc = tailer.readingDocument()) {
                            if (!dc.isPresent()) {
                                writer.append("# no more messages at ").append(Long.toHexString(dc.index())).append("\n");
                                return;
                            }
                            if (dc.index() > toIndex)
                                return;
                            writer.append("# index: ").append(Long.toHexString(dc.index())).append("\n");
                            Wire wire = dc.wire();
                            long start = wire.bytes().readPosition();
                            try {
                                text.clear();
                                wire.copyTo(text); // Copy wire contents to text wire
                                writer.append(bytes.toString());

                            } catch (Exception e) {
                                wire.bytes().readPosition(start);
                                writer.append(wire.bytes()).append("\n");
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace(new PrintWriter(writer));

        } finally {
            try {
                writer.flush(); // Ensure the writer is flushed
            } catch (IOException e) {
                Jvm.debug().on(SingleChronicleQueue.class, e);
            }
        }
    }

    /**
     * Returns the chunk count. Used in testing.
     *
     * @return the chunk count
     */
    public long chunkCount() {
        return chunkCount[0];
    }

    /**
     * Returns the number of index entries.
     *
     * @return the index count
     */
    @Override
    public int indexCount() {
        return indexCount;
    }

    /**
     * Returns the spacing between index entries.
     *
     * @return the index spacing
     */
    @Override
    public int indexSpacing() {
        return indexSpacing;
    }

    /**
     * Returns the epoch time used for roll cycles.
     *
     * @return the epoch time
     */
    @Override
    public long epoch() {
        return epoch;
    }

    /**
     * Returns the roll cycle used by the queue.
     *
     * @return the roll cycle
     */
    @Override
    @NotNull
    public RollCycle rollCycle() {
        return this.rollCycle;
    }

    /**
     * Returns the interval for delta checkpointing.
     *
     * @return the delta checkpoint interval
     */
    @Override
    public int deltaCheckpointInterval() {
        return deltaCheckpointInterval;
    }

    /**
     * Indicates whether the queue uses asynchronous buffering for appending.
     * In asynchronous mode, appends are handled by a background thread.
     *
     * @return true if the queue uses asynchronous buffering, false otherwise
     */
    public boolean buffered() {
        return this.isBuffered;
    }

    /**
     * Returns the event loop used by the queue.
     *
     * @return the event loop
     */
    @NotNull
    public EventLoop eventLoop() {
        return this.eventLoop;
    }

    /**
     * Constructs a new {@link ExcerptAppender} once the {@link #createAppenderCondition} is met.
     *
     * @return the new ExcerptAppender
     * @throws InterruptedRuntimeException if the thread is interrupted while waiting for the condition
     */
    @NotNull
    protected ExcerptAppender createNewAppenderOnceConditionIsMet() {
        try {
            createAppenderCondition.await(); // Wait for the condition to be met before creating the appender
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedRuntimeException("Interrupted waiting for condition to create appender", e);
        }
        return constructAppender();
    }

    /**
     * Constructs a new {@link ExcerptAppender}.
     * Subclasses can override this method to provide custom appender creation logic.
     * To create a new appender, subclasses should call {@link #createNewAppenderOnceConditionIsMet()}.
     *
     * @return the new ExcerptAppender
     */
    @NotNull
    protected ExcerptAppender constructAppender() {
        final WireStorePool newPool = WireStorePool.withSupplier(storeSupplier, storeFileListener);
        return new StoreAppender(this, newPool, checkInterrupts);
    }

    /**
     * Returns the StoreFileListener used by the queue.
     *
     * @return the StoreFileListener
     */
    protected StoreFileListener storeFileListener() {
        return storeFileListener;
    }

    /**
     * Returns the WireStoreSupplier used by the queue.
     * This method is used by enterprise Chronicle Queue.
     *
     * @return the WireStoreSupplier
     */
    WireStoreSupplier storeSupplier() {
        return storeSupplier;
    }

    /**
     * Acquires an {@link ExcerptAppender} from a thread-local pool of appenders.
     * If the queue is in read-only mode, an IllegalStateException is thrown.
     *
     * @return the ExcerptAppender
     */
    @SuppressWarnings("deprecation")
    @NotNull
    @Override
    public ExcerptAppender acquireAppender() {
        return ThreadLocalAppender.acquireThreadLocalAppender(this);
    }

    /**
     * Acquires a thread-local ExcerptAppender for the given queue.
     * If the queue is in read-only mode, an IllegalStateException is thrown.
     *
     * @param queue the SingleChronicleQueue for which the appender is acquired
     * @return the ExcerptAppender
     * @throws IllegalStateException if the queue is in read-only mode
     */
    @NotNull
    ExcerptAppender acquireThreadLocalAppender(@NotNull SingleChronicleQueue queue) {
        queue.throwExceptionIfClosed();
        if (queue.readOnly)
            throw new IllegalStateException("Can't append to a read-only chronicle");

        ExcerptAppender res = strongExcerptAppenderThreadLocal.get();

        if (res.isClosing())
            strongExcerptAppenderThreadLocal.set(res = createNewAppenderOnceConditionIsMet());

        return res;
    }

    /**
     * Creates a new {@link ExcerptAppender}. If the queue is in read-only mode,
     * an {@link IllegalStateException} is thrown.
     *
     * @return a new ExcerptAppender
     * @throws IllegalStateException if the queue is read-only
     */
    @NotNull
    @Override
    public ExcerptAppender createAppender() {
        throwExceptionIfClosed();

        if (readOnly)
            throw new IllegalStateException("Can't append to a read-only chronicle");

        return createNewAppenderOnceConditionIsMet();
    }

    /**
     * Returns the {@link WriteLock} used to lock writes to the queue.
     * This lock is used to coordinate writes from multiple threads and processes.
     * It should be held only briefly as it blocks any other writers.
     *
     * @return the WriteLock for the queue
     */
    @NotNull
    WriteLock writeLock() {
        return writeLock;
    }

    /**
     * Returns the {@link WriteLock} used for appends, primarily for Queue Enterprise
     * sink replication handlers.
     *
     * @return the WriteLock used for appends
     */
    public WriteLock appendLock() {
        return appendLock;
    }

    /**
     * Creates an {@link ExcerptTailer} with a specific ID. The tailer will use the
     * provided ID to track its position, and the preconditions for creating a tailer
     * are verified before initialization.
     *
     * @param id the identifier for the tailer
     * @return a new ExcerptTailer
     * @throws NamedTailerNotAvailableException if the tailer is not available due to replication locks
     */
    @NotNull
    @Override
    public ExcerptTailer createTailer(String id) {
        verifyTailerPreconditions(id); // Check preconditions for tailer creation
        IndexUpdater indexUpdater = IndexUpdaterFactory.createIndexUpdater(id, this);
        final StoreTailer storeTailer = new StoreTailer(this, pool, indexUpdater);
        directoryListing.refresh(true);
        storeTailer.singleThreadedCheckReset();
        return storeTailer;
    }

    /**
     * Verifies the preconditions before creating a tailer. Ensures the queue is not
     * closed and handles specific cases for replicated named tailers.
     *
     * @param id the identifier for the tailer, may be null for non-named tailers
     * @throws NamedTailerNotAvailableException if a named tailer is not available
     */
    private void verifyTailerPreconditions(String id) {
        // Preconditions for all tailer types
        throwExceptionIfClosed();

        if (id == null) return; // No special checks for non-named tailers
        if (appendLock.locked() && id.startsWith(REPLICATED_NAMED_TAILER_PREFIX)) {
            throw new NamedTailerNotAvailableException(id, NamedTailerNotAvailableException.Reason.NOT_AVAILABLE_ON_SINK);
        }
    }

    /**
     * Acquires a LongValue object for a given ID from the metadata store.
     * This is typically used to track indexes for specific IDs.
     *
     * @param id the identifier for which to acquire the index
     * @return a LongValue representing the index for the given ID
     */
    @Override
    @NotNull
    public LongValue indexForId(@NotNull String id) {
        return this.metaStore.doWithExclusiveLock((ts) -> ts.acquireValueFor("index." + id, 0L));
    }

    /**
     * Acquires the version index for a given ID from the metadata store.
     *
     * @param id the identifier for which to acquire the version index
     * @return a LongValue representing the version index for the given ID
     */
    @NotNull
    public LongValue indexVersionForId(@NotNull String id) {
        return this.metaStore.doWithExclusiveLock((ts) -> ts.acquireValueFor(String.format(INDEX_VERSION_FORMAT, id), -1L));
    }

    /**
     * Creates a write lock specifically for versioned indexes identified by the given ID.
     *
     * @param id the identifier for which to create the write lock
     * @return a new TableStoreWriteLock for the version index
     */
    @NotNull
    public TableStoreWriteLock versionIndexLockForId(@NotNull String id) {
        return new TableStoreWriteLock(
                metaStore,
                pauserSupplier,
                timeoutMS * 3 / 2,
                String.format(SingleChronicleQueue.INDEX_LOCK_FORMAT, id)
        );
    }

    /**
     * Creates a default {@link ExcerptTailer} for the queue. If the queue is closed,
     * an exception is thrown.
     *
     * @return a new ExcerptTailer
     */
    @NotNull
    @Override
    public ExcerptTailer createTailer() {
        throwExceptionIfClosed();

        return createTailer(null);
    }

    /**
     * Retrieves or creates a {@link SingleChronicleQueueStore} for a specific cycle.
     * The store is acquired from the pool, and if createIfAbsent is true, a new store
     * is created if it doesn't already exist.
     *
     * @param cycle the cycle for which to acquire the store
     * @param epoch the epoch time
     * @param createIfAbsent whether to create a store if it doesn't exist
     * @param oldStore the previous store, if available
     * @return the acquired or created SingleChronicleQueueStore, or null if unavailable
     */
    @Nullable
    @Override
    public final SingleChronicleQueueStore storeForCycle(int cycle, final long epoch, boolean createIfAbsent, SingleChronicleQueueStore oldStore) {
        return this.pool.acquire(cycle,
                createIfAbsent ? WireStoreSupplier.CreateStrategy.CREATE : WireStoreSupplier.CreateStrategy.READ_ONLY,
                oldStore);
    }

    /**
     * Returns the next cycle in the specified direction.
     *
     * @param cycle the current cycle
     * @param direction the direction (forward or backward) in which to find the next cycle
     * @return the next cycle
     * @throws ParseException if there is an error parsing cycle data
     */
    @Override
    public int nextCycle(int cycle, @NotNull TailerDirection direction) throws ParseException {
        throwExceptionIfClosed();

        return pool.nextCycle(cycle, direction);
    }

    /**
     * Counts the number of excerpts between two indexes. This method accounts for the possibility
     * that intermediate Chronicle files may have been removed.
     *
     * @param fromIndex the starting index
     * @param toIndex the ending index
     * @return the number of excerpts between the two indexes
     * @throws IllegalStateException if unable to read the Chronicle files
     */
    @Override
    public long countExcerpts(long fromIndex, long toIndex) {
        throwExceptionIfClosed();

        try (ExcerptTailer tailer = createTailer()) {
            if (fromIndex > toIndex) {
                long temp = fromIndex;
                fromIndex = toIndex;
                toIndex = temp;
            }

            if (fromIndex == toIndex) // If both indexes are the same
                return 0;

            long result = 0;

            // some of the sequences maybe at -1 so we will add 1 to the cycle and update the result
            // accordingly
            RollCycle rollCycle = rollCycle();
            long sequenceNotSet = rollCycle.toSequenceNumber(-1);

            // Adjust for uninitialized sequences
            if (rollCycle.toSequenceNumber(fromIndex) == sequenceNotSet) {
                result++;
                fromIndex++;
            }

            if (rollCycle.toSequenceNumber(toIndex) == sequenceNotSet) {
                result--;
                toIndex++;
            }

            int lowerCycle = rollCycle.toCycle(fromIndex);
            int upperCycle = rollCycle.toCycle(toIndex);

            // If both indexes are in the same cycle
            if (lowerCycle == upperCycle)
                return toIndex - fromIndex;

            long upperSeqNum = rollCycle.toSequenceNumber(toIndex);
            long lowerSeqNum = rollCycle.toSequenceNumber(fromIndex);

            if (lowerCycle + 1 == upperCycle) {
                long l = tailer.excerptsInCycle(lowerCycle);
                result += (l - lowerSeqNum) + upperSeqNum;
                return result;
            }

            NavigableSet<Long> cycles;
            try {
                cycles = listCyclesBetween(lowerCycle, upperCycle); // List cycles between the two indexes
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }

            // Handle the lower cycle
            if (cycles.first() == lowerCycle) {
                // because we are inclusive, for example  if we were at the end, then this
                // is 1 except rather than zero
                long l = tailer.excerptsInCycle(lowerCycle);
                result += (l - lowerSeqNum);
            } else
                throw new IllegalStateException("Cycle not found, lower-cycle=" + Long.toHexString(lowerCycle));

            // Handle the upper cycle
            if (cycles.last() == upperCycle) {
                result += upperSeqNum;
            } else
                throw new IllegalStateException("Cycle not found,  upper-cycle=" + Long.toHexString(upperCycle));

            if (cycles.size() == 2)
                return result;

            final long[] array = cycles.stream().mapToLong(i -> i).toArray();
            for (int i = 1; i < array.length - 1; i++) {
                long x = tailer.excerptsInCycle(Math.toIntExact(array[i]));
                result += x;
            }

            return result;
        }
    }

    /**
     * Lists the cycles between the specified lower and upper cycle values.
     *
     * @param lowerCycle the starting cycle
     * @param upperCycle the ending cycle
     * @return a NavigableSet of Long values representing the cycles between the lower and upper cycle
     */
    public NavigableSet<Long> listCyclesBetween(int lowerCycle, int upperCycle) {
        throwExceptionIfClosed();

        return pool.listCyclesBetween(lowerCycle, upperCycle);
    }

    /**
     * Adds a {@link Closeable} listener that will be closed when this queue is closed.
     *
     * @param key the Closeable to add to the close listeners
     */
    public <T> void addCloseListener(Closeable key) {
        synchronized (closers) {
            if (!closers.isEmpty())
                closers.removeIf(Closeable::isClosed); // Remove already closed listeners
            closers.add(key); // Add the new listener
        }
    }

    /**
     * Performs the closing operations for this queue. All resources are closed in a synchronized
     * block, and special care is taken to close the event loop and other important components.
     */
    @Override
    protected void performClose() {
        synchronized (closers) {
            metaStoreMap.values().forEach(Closeable::closeQuietly); // Close all metaStoreMap entries
            metaStoreMap.clear();
            closers.forEach(Closeable::closeQuietly); // Close all closers
            closers.clear();

            // must be closed after closers.
            closeQuietly(
                    createAppenderCondition,
                    directoryListing,
                    lastAcknowledgedIndexReplicated,
                    lastIndexReplicated,
                    lastIndexMSynced,
                    writeLock,
                    appendLock,
                    pool,
                    metaStore);
            closeQuietly(storeSupplier);
        }

        // If the event loop was created on demand, close it
        if (eventLoop instanceof OnDemandEventLoop)
            eventLoop.close();
    }

    /**
     * Ensures that resources are properly closed during finalization if they were not closed earlier.
     *
     * @throws Throwable if there is an error during finalization
     */
    @SuppressWarnings({"deprecation", "removal"})
    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        warnAndCloseIfNotClosed();
    }

    /**
     * Closes the specified {@link SingleChronicleQueueStore}.
     *
     * @param store the store to close, may be null
     */
    public final void closeStore(@Nullable SingleChronicleQueueStore store) {
        if (store != null)
            this.pool.closeStore(store);
    }

    /**
     * Returns the current cycle based on the roll cycle, time provider, and epoch.
     *
     * @return the current cycle
     */
    @Override
    public final int cycle() {
        return cycleCalculator.currentCycle(rollCycle, time, epoch);
    }

    /**
     * Returns the current cycle using a specified time provider.
     *
     * @param timeProvider the TimeProvider to use for cycle calculation
     * @return the current cycle
     */
    public final int cycle(TimeProvider timeProvider) {
        return cycleCalculator.currentCycle(rollCycle, timeProvider, epoch);
    }

    /**
     * Returns the first index of the queue. If the first cycle is not available, returns Long.MAX_VALUE.
     *
     * @return the first index of the queue
     */
    @Override
    public long firstIndex() {
        int cycle = firstCycle();
        if (cycle == Integer.MAX_VALUE)
            return Long.MAX_VALUE;

        return rollCycle().toIndex(cycle, 0);
    }

    /**
     * Returns the last index in the queue. This is a slow implementation that uses
     * a {@link ExcerptTailer} to find the last non-metadata document.
     *
     * @return the last index in the queue, or -1 if no documents are found
     */
    @Override
    public long lastIndex() {
        // This is a slow implementation that gets a Tailer/DocumentContext to find the last index
        try (final ExcerptTailer tailer = createTailer().direction(BACKWARD).toEnd()) {
            while (true) {
                try (final DocumentContext documentContext = tailer.readingDocument()) {
                    if (documentContext.isPresent()) {
                        if (!documentContext.isMetaData())
                            return documentContext.index(); // Return the index of the last document
                    } else {
                        return -1; // No documents found
                    }
                }
            }
        }
    }

    /**
     * Counts the number of messages in the queue by calculating the number of excerpts
     * between the start (firstIndex) and end (lastIndex) of the queue.
     *
     * @return the number of messages in the queue
     */
    @Override
    public long entryCount() {
        try (final ExcerptTailer tailer = createTailer()) {
            tailer.toEnd();
            long lastIndex = tailer.index();
            if (lastIndex == 0)
                return 0;
            return countExcerpts(firstIndex(), lastIndex);
        }
    }

    /**
     * Returns the list of files in the queue's directory.
     *
     * @return an array of file names in the directory, or null if an error occurs
     */
    @Nullable
    String[] getList() {
        return path.list();
    }

    /**
     * Sets the first and last cycle values based on the current system time and the directory listing.
     * The directory listing is refreshed if necessary, either periodically or forced based on the time.
     */
    private void setFirstAndLastCycle() {
        long now = System.currentTimeMillis();
        if (now <= directoryListing.lastRefreshTimeMS()) {
            return;
        }

        boolean force = now - directoryListing.lastRefreshTimeMS() > forceDirectoryListingRefreshIntervalMs;
        directoryListing.refresh(force); // Force refresh if the interval has passed
    }

    /**
     * Returns the first cycle available in the queue by setting the first and last cycle
     * and then retrieving the minimum created cycle from the directory listing.
     *
     * @return the first cycle in the queue
     */
    @Override
    public int firstCycle() {
        setFirstAndLastCycle();
        return directoryListing.getMinCreatedCycle();
    }

    /**
     * allows the appenders to inform the queue that they have rolled
     *
     * @param cycle the cycle the appender has rolled to
     */
    void onRoll(int cycle) {
        directoryListing.onRoll(cycle);
    }

    /**
     * Returns the last cycle available in the queue by setting the first and last cycle
     * and then retrieving the maximum created cycle from the directory listing.
     *
     * @return the last cycle in the queue
     */
    @Override
    public int lastCycle() {
        setFirstAndLastCycle();
        return directoryListing.getMaxCreatedCycle();
    }

    /**
     * Returns the consumer that handles {@link BytesRingBufferStats}.
     *
     * @return the consumer for ring buffer statistics
     */
    @NotNull
    public Consumer<BytesRingBufferStats> onRingBufferStats() {
        return this.onRingBufferStats;
    }

    /**
     * Returns the block size used by the queue.
     *
     * @return the block size
     */
    public long blockSize() {
        return this.blockSize;
    }

    /**
     * Returns the overlap size for memory mapping.
     *
     * @return the overlap size
     */
    public long overlapSize() {
        return this.overlapSize;
    }

    /**
     * Returns the {@link WireType} used by the queue.
     *
     * @return the WireType
     */
    @NotNull
    @Override
    public WireType wireType() {
        return wireType;
    }

    /**
     * Returns the buffer capacity used by the queue.
     *
     * @return the buffer capacity
     */
    public long bufferCapacity() {
        return this.bufferCapacity;
    }

    /**
     * Creates a {@link MappedFile} for the given file. The mapped file is created using the
     * block size and overlap size for memory mapping. Sync mode is also set for the file.
     *
     * @param file the file to map
     * @return the MappedFile instance
     * @throws FileNotFoundException if the file cannot be found
     */
    @NotNull
    @PackageLocal
    MappedFile mappedFile(File file) throws FileNotFoundException {
        long chunkSize = OS.pageAlign(blockSize); // Align the chunk size to OS page size
        final MappedFile mappedFile = MappedFile.of(file, chunkSize, overlapSize, readOnly);
        mappedFile.syncMode(syncMode);
        return mappedFile;
    }

    /**
     * Returns whether the queue is in read-only mode.
     *
     * @return true if the queue is read-only, false otherwise
     */
    boolean isReadOnly() {
        return readOnly;
    }

    /**
     * Returns a string representation of the queue, showing the source ID and file path.
     *
     * @return a string representation of the queue
     */
    @NotNull
    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{" +
                "sourceId=" + sourceId +
                ", file=" + path +
                '}';
    }

    /**
     * Returns the {@link TimeProvider} used by the queue.
     *
     * @return the TimeProvider
     */
    @NotNull
    public TimeProvider time() {
        return time;
    }

    /**
     * Creates a function that converts a file name into a cycle by parsing the name and removing the suffix.
     *
     * @return a function that converts a file name to a cycle
     */
    @NotNull
    private ToIntFunction<String> fileNameToCycleFunction() {
        return name -> dateCache.parseCount(name.substring(0, name.length() - SUFFIX.length()));
    }

    /**
     * Removes the specified {@link StoreTailer} from the close listeners.
     *
     * @param storeTailer the StoreTailer to remove
     */
    void removeCloseListener(final StoreTailer storeTailer) {
        synchronized (closers) {
            closers.remove(storeTailer);
        }
    }

    /**
     * Returns the metadata store used by the queue.
     *
     * @return the TableStore for metadata
     */
    public TableStore<SCQMeta> metaStore() {
        return metaStore;
    }

    /**
     * Puts a new value in the table store for the given key and index. If the index is Long.MIN_VALUE,
     * it sets the value as volatile, otherwise, it sets the max value.
     *
     * @param key   the key for the entry in the table store
     * @param index the index value to set
     */
    public void tableStorePut(CharSequence key, long index) {
        LongValue longValue = tableStoreAcquire(key, index);
        if (longValue == null) return;
        if (index == Long.MIN_VALUE)
            longValue.setVolatileValue(index);
        else
            longValue.setMaxValue(index);
    }

    /**
     * Acquires a {@link LongValue} from the table store for the given key, creating a new one
     * if necessary. If the key does not exist, a new LongValue is created with the default value.
     *
     * @param key          the key for the entry
     * @param defaultValue the default value to use if the key does not exist
     * @return the acquired LongValue, or null if an error occurs
     */
    @Nullable
    protected LongValue tableStoreAcquire(CharSequence key, long defaultValue) {
        try (final ScopedResource<Bytes<Void>> bytesTl = acquireBytesScoped()) {
            BytesStore<?, ?> keyBytes = asBytes(key, bytesTl.get());
            LongValue longValue = metaStoreMap.get(keyBytes);
            if (longValue == null) {
                synchronized (closers) {
                    longValue = metaStoreMap.get(keyBytes);
                    if (longValue == null) {
                        longValue = metaStore.acquireValueFor(key, defaultValue);
                        int length = key.length();
                        HeapBytesStore<byte[]> key2 = HeapBytesStore.wrap(new byte[length]);
                        key2.write(0, keyBytes, 0, length);
                        metaStoreMap.put(key2, longValue); // Store the key and value in metaStoreMap
                        return longValue;
                    }
                }
            }
            return longValue;
        }
    }

    /**
     * Gets the value for the given key from the table store. If the key does not exist,
     * returns Long.MIN_VALUE.
     *
     * @param key the key for the entry in the table store
     * @return the value associated with the key, or Long.MIN_VALUE if not found
     */
    public long tableStoreGet(CharSequence key) {
        LongValue longValue = tableStoreAcquire(key, Long.MIN_VALUE);
        if (longValue == null) return Long.MIN_VALUE;
        return longValue.getVolatileValue();
    }

    /**
     * Converts a {@link CharSequence} key into a {@link BytesStore}. If the key is already
     * a BytesStore, it is cast, otherwise the key is appended to a Bytes instance.
     *
     * @param key   the key to convert
     * @param bytes the Bytes instance used for conversion
     * @return the BytesStore representation of the key
     */
    @SuppressWarnings("unchecked")
    private BytesStore<?,Void> asBytes(CharSequence key, Bytes<Void> bytes) {
        return key instanceof BytesStore
                ? ((BytesStore<?, Void>) key)
                : bytes.append(key);
    }

    /**
     * A cached structure that stores cycle-related information, including the directory modification count
     * and a map of cycle numbers to files.
     */
    private static final class CachedCycleTree {
        private final long directoryModCount;
        private final NavigableMap<Long, File> cachedCycleTree;

        /**
         * Constructs a CachedCycleTree with the specified directory modification count and cached cycle tree.
         *
         * @param directoryModCount the modification count of the directory
         * @param cachedCycleTree   the cached map of cycles to files
         */
        CachedCycleTree(final long directoryModCount, final NavigableMap<Long, File> cachedCycleTree) {
            this.directoryModCount = directoryModCount;
            this.cachedCycleTree = cachedCycleTree;
        }
    }

    /**
     * StoreSupplier is responsible for supplying {@link SingleChronicleQueueStore} instances
     * for specific cycles. It manages the mapping of files to memory and caches these mappings.
     * This class also handles the creation and retrieval of stores for different cycles.
     */
    class StoreSupplier extends AbstractCloseable implements WireStoreSupplier {

        // A cached tree structure to store cycle-related data.
        private final AtomicReference<CachedCycleTree> cachedTree = new AtomicReference<>();

        // A cache for managing MappedFile and MappedBytes, used to map files into memory.
        private final ReferenceCountedCache<File, MappedFile, MappedBytes, IOException> mappedFileCache;

        // Indicates whether the queue path exists on disk.
        private boolean queuePathExists;

        /**
         * Constructor for StoreSupplier. It initializes the mapped file cache
         * and disables single-threaded checks.
         */
        private StoreSupplier() {
            mappedFileCache = new ReferenceCountedCache<>(
                    MappedBytes::mappedBytes,
                    SingleChronicleQueue.this::mappedFile); // Set up cache with mapping function
            singleThreadedCheckDisabled(true); // Disable single-threaded check
        }

        /**
         * Acquires a {@link SingleChronicleQueueStore} for the specified cycle.
         * If the store doesn't exist and the strategy is {@link CreateStrategy.CREATE}, it will create a new store.
         *
         * @param cycle          the cycle to acquire the store for
         * @param createStrategy the strategy for creating or reading the store
         * @return the acquired SingleChronicleQueueStore or null if the store doesn't exist and the strategy is not CREATE
         * @throws IOException in case of IO errors
         * @throws TimeoutException if acquiring the store times out
         */
        @Override
        public SingleChronicleQueueStore acquire(int cycle, CreateStrategy createStrategy) {
            throwExceptionIfClosed();

            SingleChronicleQueue that = SingleChronicleQueue.this;
            @NotNull final RollingResourcesCache.Resource dateValue = that
                    .dateCache.resourceFor(cycle);
            MappedBytes mappedBytes = null;
            try {
                File path = dateValue.path;

                // Refresh the directory listing to ensure it's up to date
                directoryListing.refresh(false);

                // Check if the store exists for the given cycle
                if (createStrategy != CreateStrategy.CREATE &&
                        (cycle > directoryListing.getMaxCreatedCycle()
                                || cycle < directoryListing.getMinCreatedCycle()
                                || !path.exists())) {
                    return null;
                }

                throwExceptionIfClosed();

                // Handle the creation of the store if it doesn't exist
                if (createStrategy == CreateStrategy.CREATE && !path.exists() && !dateValue.pathExists)
                    PrecreatedFiles.renamePreCreatedFileToRequiredFile(path);

                dateValue.pathExists = true;

                try {
                    mappedBytes = mappedFileCache.get(path); // Retrieve mapped bytes for the file
                } catch (FileNotFoundException e) {
                    createFile(path); // Create the file if it doesn't exist
                    mappedBytes = mappedFileCache.get(path);
                }
                mappedBytes.singleThreadedCheckDisabled(true);
                mappedBytes.chunkCount(chunkCount);

//                pauseUnderload();

                if (SHOULD_CHECK_CYCLE && cycle != rollCycle.current(time, epoch)) {
                    Jvm.warn().on(getClass(), new Exception("Creating cycle which is not the current cycle"));
                }
                queuePathExists = true;
                Wire wire = wireType.apply(mappedBytes); // Create wire from mapped bytes
                wire.pauser(pauserSupplier.get()); // Set the pauser for the wire
                wire.headerNumber(rollCycle.toIndex(cycle, 0)); // Set the header number

                SingleChronicleQueueStore wireStore;
                try {
                    if (!readOnly && createStrategy == CreateStrategy.CREATE && wire.writeFirstHeader()) {
                        // Create a new store and update the header
                        wireStore = storeFactory.apply(that, wire);

                        createIndexThenUpdateHeader(wire, cycle, wireStore);
                    } else {
                        try {
                            wire.readFirstHeader(timeoutMS, TimeUnit.MILLISECONDS); // Read the first header
                        } catch (TimeoutException e) {
                            File cycleFile = mappedBytes.mappedFile().file();

                            mappedBytes.close();
                            mappedFileCache.remove(path);

                            if (!readOnly && createStrategy != CreateStrategy.READ_ONLY && cycleFileRenamed != cycle) {
                                SingleChronicleQueueStore acquired = acquire(cycle, backupCycleFile(cycle, cycleFile));

                                if (acquired == null)
                                    throw e;

                                return acquired;
                            }

                            if (Jvm.debug().isEnabled(SingleChronicleQueue.class)) {
                                Jvm.debug().on(SingleChronicleQueue.class, "Cycle file not ready: " + cycleFile.getAbsolutePath());
                            }
                            return null;
                        }

                        final ValueIn valueIn = readWireStoreValue(wire);
                        try {
                            wireStore = valueIn.typedMarshallable(); // Deserialize the wire store
                        } catch (Throwable t) {
                            mappedBytes.close();
                            throw t;
                        }
                    }
                } catch (InternalError e) {
                    long pos = Objects.requireNonNull(((Bytes<?>) mappedBytes).bytesStore()).addressForRead(0);
                    String s = Long.toHexString(pos);
                    System.err.println("pos=" + s);
                    try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("/proc/self/maps")))) {
                        for (String line; (line = br.readLine()) != null; )
                            if (line.contains(".cq4"))
                                System.err.println(line);
                    }
//                        System.err.println("wire.bytes.refCount="+wire.bytes().refCount());
//                        System.err.println("wire.bytes.byteStore.refCount="+wire.bytes().bytesStore().refCount());
                    throw e;
                }
                return wireStore;

            } catch (@NotNull TimeoutException | IOException e) {
                Closeable.closeQuietly(mappedBytes);
                throw Jvm.rethrow(e);
            }
        }

        /**
         * Reads the value of the wire store from the wire. Ensures the first message
         * is the header, and throws an exception if the header is not present.
         *
         * @param wire the Wire to read from
         * @return the ValueIn object containing the wire store value
         * @throws StreamCorruptedException if the first message is not the header
         */
        @NotNull
        private ValueIn readWireStoreValue(@NotNull Wire wire) throws StreamCorruptedException {
            try (ScopedResource<StringBuilder> stlSb = Wires.acquireStringBuilderScoped()) {
                StringBuilder name = stlSb.get();
                ValueIn valueIn = wire.readEventName(name);
                if (!StringUtils.isEqual(name, MetaDataKeys.header.name())) {
                    throw new StreamCorruptedException("The first message should be the header, was " + name);
                }
                return valueIn;
            }
        }

        /**
         * Renames an unacquirable cycle file to a backup file with a discard suffix, and attempts to recreate the segment.
         *
         * @param cycle     the cycle number
         * @param cycleFile the file that couldn't be acquired
         * @return the strategy to either create a new file or use it as read-only
         */
        private CreateStrategy backupCycleFile(int cycle, File cycleFile) {
            File cycleFileDiscard = new File(cycleFile.getParentFile(),
                    String.format("%s-%d%s", cycleFile.getName(), System.currentTimeMillis(), DISCARD_FILE_SUFFIX));
            boolean success = cycleFile.renameTo(cycleFileDiscard); // Rename the file

            // Prevent renaming the same cycle multiple times from the same queue
            if (success)
                cycleFileRenamed = cycle;

            Jvm.warn().on(SingleChronicleQueue.class, "Renamed un-acquirable segment file to " +
                    cycleFileDiscard.getAbsolutePath() + ": " + success);

            return success ? CreateStrategy.CREATE : CreateStrategy.READ_ONLY;
        }

        /**
         * This method initializes the index for the wire store and updates the header.
         * It ensures that all data structures are properly prepared before publishing the initial header.
         * It also notifies the directory listing of the new file creation.
         *
         * @param wire      the Wire object used for writing
         * @param cycle     the cycle number for which the index is created
         * @param wireStore the wire store for the current cycle
         */
        @SuppressWarnings("deprecation")
        private void createIndexThenUpdateHeader(Wire wire, int cycle, SingleChronicleQueueStore wireStore) {
            // Enable padding if the data version is greater than 0
            wire.usePadding(wireStore.dataVersion() > 0);
            wire.padToCacheAlign(); // Align to cache boundary

            // Record the end position of the header
            long headerEndPos = wire.bytes().writePosition();
            wireStore.initIndex(wire); // Initialize the index for the wire store
            wire.updateFirstHeader(headerEndPos); // Update the header with the correct position

            // Reset write position to the header size for subsequent writes
            wire.bytes().writePosition(SPB_HEADER_SIZE);

            // Notify the directory listing of the newly created file
            directoryListing.onFileCreated(path, cycle);
        }

        /**
         * Closes the StoreSupplier by releasing the mapped file cache resources.
         */
        @Override
        protected void performClose() {
            mappedFileCache.close(); // Close all cached mapped files
        }

        /**
         * Creates a new file at the specified path. If the parent directory does not exist, it is created.
         *
         * @param path the path of the file to create
         */
        private void createFile(final File path) {
            try {
                File dir = path.getParentFile();
                if (!dir.exists())
                    dir.mkdirs(); // Create the parent directory if it doesn't exist

                // Attempt to create the file
                if (!path.createNewFile()) {
                    Jvm.warn().on(getClass(), "unable to create a file at " + path.getAbsolutePath());
                }
            } catch (IOException ex) {
                Jvm.warn().on(getClass(), "unable to create a file at " + path.getAbsolutePath(), ex);
            }
        }

        /**
         * Returns a map of cycle files in the current directory. If necessary, it refreshes
         * the directory listing and updates the cache.
         *
         * @param force whether to forcefully refresh the directory listing
         * @return a NavigableMap of cycle numbers and their corresponding files
         */
        @NotNull
        private NavigableMap<Long, File> cycleTree(final boolean force) {

            final File parentFile = path;

            // use pre-calculated result in case where queue dir existed when StoreSupplier was constructed
            if (!queuePathExists && !parentFile.exists())
                throw new IllegalStateException("parentFile=" + parentFile.getName() + " does not exist");

            // Retrieve the cached cycle tree
            CachedCycleTree cachedValue = cachedTree.get();
            final long directoryModCount = directoryListing.modCount();
            if (force || (cachedValue == null || directoryModCount == -1 || directoryModCount > cachedValue.directoryModCount)) {

                final RollingResourcesCache dateCache = SingleChronicleQueue.this.dateCache;
                final NavigableMap<Long, File> tree = new TreeMap<>();

                // Populate the tree with cycle files
                final File[] files = parentFile.listFiles((File file) -> file.getPath().endsWith(SUFFIX));
                if (files != null)
                    for (File file : files)
                        tree.put(dateCache.toLong(file), file);

                cachedValue = new CachedCycleTree(directoryModCount, tree);

                // Update the cached tree atomically
                while (true) {
                    final CachedCycleTree existing = cachedTree.get();

                    if (existing != null && existing.directoryModCount > cachedValue.directoryModCount) {
                        break;
                    }

                    if (cachedTree.compareAndSet(existing, cachedValue)) {
                        break;
                    }
                    Jvm.nanoPause();
                }
            }

            return cachedValue.cachedCycleTree;
        }

        /**
         * Finds the next cycle in the given direction. If the current cycle is no longer
         * present in the tree, it logs an error.
         *
         * @param currentCycle the current cycle
         * @param direction    the direction to move (FORWARD or BACKWARD)
         * @return the next cycle in the given direction
         */
        @Override
        public int nextCycle(int currentCycle, @NotNull TailerDirection direction) {
            throwExceptionIfClosed();

            if (direction == NONE)
                throw new AssertionError("direction is NONE");
            assert currentCycle >= 0 : "currentCycle=" + Integer.toHexString(currentCycle);

            // Get the cycle tree and confirm the current cycle is within range
            NavigableMap<Long, File> tree = cycleTree(false);
            final File currentCycleFile = dateCache.resourceFor(currentCycle).path;

            directoryListing.refresh(false); // Refresh the directory listing
            if (currentCycle > directoryListing.getMaxCreatedCycle() ||
                    currentCycle < directoryListing.getMinCreatedCycle()) {
                for (int i = 0; i < 20; i++) {
                    Jvm.pause(10);
                    directoryListing.refresh(i > 1); // Force refresh if necessary
                    if (currentCycle <= directoryListing.getMaxCreatedCycle() &&
                            currentCycle >= directoryListing.getMinCreatedCycle()) {
                        break;
                    }
                }
            }

            // Try to find the current cycle in the tree
            Long key = dateCache.toLong(currentCycleFile);
            File file = tree.get(key);
            if (file == null) {
                tree = cycleTree(true); // Force a refresh if the cycle is not found
                file = tree.get(key);
            }

            // Log an error if the current cycle is missing from the tree
            if (file == null) {
                Jvm.error().on(SingleChronicleQueue.class, "The current cycle seems to have been deleted from under the queue, scanning to find the next remaining cycle, currentCycle=" + currentCycleFile);
            }

            // Move to the next cycle in the specified direction
            switch (direction) {
                case FORWARD:
                    return toCycle(tree.higherEntry(key));
                case BACKWARD:
                    return toCycle(tree.lowerEntry(key));
                default:
                    throw new UnsupportedOperationException("Unsupported Direction");
            }
        }

        /**
         * Converts a map entry to a cycle number.
         *
         * @param entry the map entry
         * @return the cycle number, or -1 if the entry is null
         */
        private int toCycle(@Nullable Map.Entry<Long, File> entry) {
            if (entry == null || entry.getValue() == null)
                return -1;
            return dateCache.parseCount(fileToText().apply(entry.getValue()));
        }

        /**
         * Returns a set of cycles between the given lower and upper cycle numbers, inclusive.
         *
         * @param lowerCycle the lower cycle (inclusive)
         * @param upperCycle the upper cycle (inclusive)
         * @return a NavigableSet of cycles between the given range
         */
        @Override
        public NavigableSet<Long> cycles(int lowerCycle, int upperCycle) {
            throwExceptionIfClosed();

            final NavigableMap<Long, File> tree = cycleTree(false);
            final Long lowerKey = toKey(lowerCycle, "lowerCycle");
            final Long upperKey = toKey(upperCycle, "upperCycle");
            assert lowerKey != null;
            assert upperKey != null;
            return tree.subMap(lowerKey, true, upperKey, true).navigableKeySet(); // Return the range of cycles
        }

        /**
         * Determines if a {@link SingleChronicleQueueStore} can be reused based on its cycle
         * and the current directory listing.
         *
         * @param store the store to check
         * @return true if the store can be reused, false otherwise
         */
        @Override
        public boolean canBeReused(@NotNull SingleChronicleQueueStore store) {
            setFirstAndLastCycle(); // Update the cycle range
            int cycle = store.cycle();
            return !store.isClosed() && cycle >= directoryListing.getMinCreatedCycle() && cycle <= directoryListing.getMaxCreatedCycle();
        }

        /**
         * Converts a cycle number to a key used in the cycle tree.
         *
         * @param cyle the cycle number
         * @param m the label to use in case of an error
         * @return the key for the cycle tree
         */
        private Long toKey(int cyle, String m) {
            final File file = dateCache.resourceFor(cyle).path;
            if (!file.exists())
                throw new IllegalStateException("'file not found' for the " + m + ", file=" + file);
            return dateCache.toLong(file);
        }
    }
}
