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

@SuppressWarnings("this-escape")
public class SingleChronicleQueue extends AbstractCloseable implements RollingChronicleQueue {

    public static final String SUFFIX = ".cq4";
    public static final String DISCARD_FILE_SUFFIX = ".discard";
    public static final String QUEUE_METADATA_FILE = "metadata" + SingleTableStore.SUFFIX;
    public static final String DISK_SPACE_CHECKER_NAME = DiskSpaceMonitor.DISK_SPACE_CHECKER_NAME;
    public static final String REPLICATED_NAMED_TAILER_PREFIX = "replicated:";
    public static final String INDEX_LOCK_FORMAT = "index.%s.lock";
    public static final String INDEX_VERSION_FORMAT = "index.%s.version";

    private static final boolean SHOULD_CHECK_CYCLE = Jvm.getBoolean("chronicle.queue.checkrollcycle");
    static final int WARN_SLOW_APPENDER_MS = Jvm.getInteger("chronicle.queue.warnSlowAppenderMs", 100);

    @NotNull
    protected final EventLoop eventLoop;
    @NotNull
    protected final TableStore<SCQMeta> metaStore;
    @NotNull
    protected final WireStorePool pool;
    protected final boolean doubleBuffer;
    final Supplier<TimingPauser> pauserSupplier;
    final long timeoutMS;
    @NotNull
    final File path;
    final String fileAbsolutePath;
    // Uses this.closers as a lock. concurrent read, locking for write.
    @SuppressWarnings("rawtypes")
    private final Map<BytesStore, LongValue> metaStoreMap = new ConcurrentHashMap<>();
    private final StoreSupplier storeSupplier;
    private final long epoch;
    private final boolean isBuffered;
    @NotNull
    private final WireType wireType;
    private final long blockSize;
    private final long overlapSize;
    @NotNull
    private final Consumer<BytesRingBufferStats> onRingBufferStats;
    private final long bufferCapacity;
    private final int indexSpacing;
    private final int indexCount;
    @NotNull
    private final TimeProvider time;
    @NotNull
    private final BiFunction<RollingChronicleQueue, Wire, SingleChronicleQueueStore> storeFactory;
    private final Set<Closeable> closers = Collections.newSetFromMap(new IdentityHashMap<>());
    private final boolean readOnly;
    @NotNull
    private final CycleCalculator cycleCalculator;
    @Nullable
    private final LongValue lastAcknowledgedIndexReplicated;
    @Nullable
    private final LongValue lastIndexReplicated;
    private final LongValue lastIndexMSynced;
    @NotNull
    private final DirectoryListing directoryListing;
    @NotNull
    private final WriteLock writeLock;
    private final boolean checkInterrupts;
    @NotNull
    private final RollingResourcesCache dateCache;
    private final WriteLock appendLock;
    private final StoreFileListener storeFileListener;
    @NotNull
    private final RollCycle rollCycle;
    final AppenderListener appenderListener;
    protected int sourceId;
    private int cycleFileRenamed = -1;
    @NotNull
    private Condition createAppenderCondition = NoOpCondition.INSTANCE;
    protected final ThreadLocal<ExcerptAppender> strongExcerptAppenderThreadLocal = CleaningThreadLocal.withCloseQuietly(this::createNewAppenderOnceConditionIsMet);
    private final long forceDirectoryListingRefreshIntervalMs;
    private final long[] chunkCount = {0};
    private final SyncMode syncMode;

    protected SingleChronicleQueue(@NotNull final SingleChronicleQueueBuilder builder) {
        try {
            rollCycle = builder.rollCycle();
            cycleCalculator = cycleCalculator(builder.rollTimeZone());
            long epoch0 = builder.epoch();
            epoch = epoch0 == 0 ? rollCycle.defaultEpoch() : epoch0;
            dateCache = new RollingResourcesCache(rollCycle, epoch, textToFile(builder), fileToText());

            storeFileListener = builder.storeFileListener();
            storeSupplier = new StoreSupplier();
            pool = WireStorePool.withSupplier(storeSupplier, storeFileListener);
            isBuffered = BufferMode.Asynchronous == builder.writeBufferMode();
            path = builder.path();
            if (!builder.readOnly())
                //noinspection ResultOfMethodCallIgnored
                path.mkdirs();
            fileAbsolutePath = path.getAbsolutePath();
            wireType = builder.wireType();
            blockSize = builder.blockSize();
            // the maximum message size is 1L << 30 so greater overlapSize has no effect
            overlapSize = calcOverlapSize(blockSize);
            eventLoop = builder.eventLoop();
            bufferCapacity = builder.bufferCapacity();
            onRingBufferStats = builder.onRingBufferStats();
            indexCount = builder.indexCount();
            indexSpacing = builder.indexSpacing();
            time = builder.timeProvider();
            pauserSupplier = builder.pauserSupplier();
            // add a 20% random element to make it less likely threads will timeout at the same time.
            timeoutMS = (long) (builder.timeoutMS() * (1 + 0.2 * new SecureRandom().nextFloat())); // Not time critical
            storeFactory = builder.storeFactory();
            checkInterrupts = builder.checkInterrupts();
            metaStore = builder.metaStore();
            doubleBuffer = builder.doubleBuffer();
            syncMode = builder.syncMode();
            if (metaStore.readOnly() && !builder.readOnly()) {
                Jvm.warn().on(getClass(), "Forcing queue to be readOnly file=" + path);
                // need to set this on builder as it is used elsewhere
                builder.readOnly(metaStore.readOnly());
            }
            readOnly = builder.readOnly();
            appenderListener = builder.appenderListener();

            if (metaStore.readOnly()) {
                this.directoryListing = new FileSystemDirectoryListing(path, fileNameToCycleFunction(), time);
            } else {
                this.directoryListing = readOnly
                        ? new TableDirectoryListingReadOnly(metaStore, time)
                        : new TableDirectoryListing(metaStore, path.toPath(), fileNameToCycleFunction(), time);
                directoryListing.init();
            }

            this.directoryListing.refresh(true);
            this.writeLock = builder.writeLock();

            // release the write lock if the process is dead
            if (writeLock instanceof TableStoreWriteLock) {
                writeLock.forceUnlockIfProcessIsDead();
            }

            this.appendLock = builder.appendLock();

            if (readOnly) {
                this.lastIndexReplicated = null;
                this.lastAcknowledgedIndexReplicated = null;
                this.lastIndexMSynced = null;
            } else {
                this.lastIndexReplicated = metaStore.doWithExclusiveLock(ts -> ts.acquireValueFor("chronicle.lastIndexReplicated", -1L));
                this.lastAcknowledgedIndexReplicated = metaStore.doWithExclusiveLock(ts -> ts.acquireValueFor("chronicle.lastAcknowledgedIndexReplicated", -1L));
                this.lastIndexMSynced = metaStore.doWithExclusiveLock(ts -> ts.acquireValueFor("chronicle.lastIndexMSynced", -1L));
            }

            this.forceDirectoryListingRefreshIntervalMs = builder.forceDirectoryListingRefreshIntervalMs();

            sourceId = builder.sourceId();

            DiskSpaceMonitor.INSTANCE.pollDiskSpace(path);

            Announcer.announce("net.openhft", "chronicle-queue",
                    AnalyticsFacade.isEnabled()
                            ? singletonMap("Analytics", "Chronicle Queue reports usage statistics. Learn more or turn off: https://github.com/OpenHFT/Chronicle-Queue/blob/ea/DISCLAIMER.adoc")
                            : emptyMap());

            final Map<String, String> additionalEventParameters = AnalyticsFacade.standardAdditionalProperties();
            additionalEventParameters.put("wire_type", wireType.toString());
            final String rollCycleName = rollCycle.toString();
            if (!rollCycleName.startsWith("TEST"))
                additionalEventParameters.put("roll_cycle", rollCycleName);

            AnalyticsHolder.instance().sendEvent("started", additionalEventParameters);

            singleThreadedCheckDisabled(true);////
        } catch (Throwable t) {
            close();
            throw Jvm.rethrow(t);
        }
    }

    private static long calcOverlapSize(long blockSize) {
        final long overlapSize;
        if (blockSize < OS.SAFE_PAGE_SIZE)
            overlapSize = blockSize;
        else if (blockSize < OS.SAFE_PAGE_SIZE * 4)
            overlapSize = OS.SAFE_PAGE_SIZE;
        else if (blockSize < 4L << 30)
            overlapSize = blockSize / 4;
        else
            overlapSize = 1L << 30;
        return overlapSize;
    }

    protected void createAppenderCondition(@NotNull Condition createAppenderCondition) {
        this.createAppenderCondition = createAppenderCondition;
    }

    protected CycleCalculator cycleCalculator(ZoneId zoneId) {
        return DefaultCycleCalculator.INSTANCE;
    }

    @NotNull
    private Function<String, File> textToFile(@NotNull SingleChronicleQueueBuilder builder) {
        return name -> new File(builder.path(), name + SUFFIX);
    }

    @NotNull
    private Function<File, String> fileToText() {
        return file -> {
            String name = file.getName();
            return name.substring(0, name.length() - SUFFIX.length());
        };
    }

    @Override
    public int sourceId() {
        return sourceId;
    }

    /**
     * when using replication to another host, this is the highest last index that has been confirmed to have been read by all of the remote host(s).
     */
    @Override
    public long lastAcknowledgedIndexReplicated() {
        return lastAcknowledgedIndexReplicated == null ? -1 : lastAcknowledgedIndexReplicated.getVolatileValue(-1);
    }

    @Override
    public void lastAcknowledgedIndexReplicated(long newValue) {
        if (lastAcknowledgedIndexReplicated != null)
            lastAcknowledgedIndexReplicated.setMaxValue(newValue);
    }

    @Override
    public void refreshDirectoryListing() {
        throwExceptionIfClosed();

        directoryListing.refresh(true);
    }

    /**
     * when using replication to another host, this is the maximum last index that has been sent to any of the remote host(s).
     */
    @Override
    public long lastIndexReplicated() {
        return lastIndexReplicated == null ? -1 : lastIndexReplicated.getVolatileValue(-1);
    }

    @Override
    public void lastIndexReplicated(long indexReplicated) {
        if (lastIndexReplicated != null)
            lastIndexReplicated.setMaxValue(indexReplicated);
    }

    @Override
    public long lastIndexMSynced() {
        return lastIndexMSynced == null ? -1 : lastIndexMSynced.getVolatileValue(-1);
    }

    @Override
    public void lastIndexMSynced(long lastIndexMSynced) {
        if (this.lastIndexMSynced != null)
            this.lastIndexMSynced.setMaxValue(lastIndexMSynced);
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    @NotNull
    public File file() {
        return path;
    }

    @NotNull
    @Override
    public String fileAbsolutePath() {
        return fileAbsolutePath;
    }

    @Override
    public @NotNull String dumpLastHeader() {
        StringBuilder sb = new StringBuilder(256);
        try (SingleChronicleQueueStore wireStore = storeForCycle(lastCycle(), epoch, false, null)) {
            sb.append(wireStore.dumpHeader());
        }
        return sb.toString();
    }

    @NotNull
    @Override
    public String dump() {
        StringBuilder sb = new StringBuilder(1024);
        sb.append(metaStore.dump(wireType));
        for (int i = firstCycle(), max = lastCycle(); i <= max; i++) {
            try (SingleChronicleQueueStore commonStore = storeForCycle(i, epoch, false, null)) {
                if (commonStore != null)
                    sb.append(commonStore.dump(wireType));
            }
        }
        return sb.toString();
    }

    @Override
    public void dump(@NotNull Writer writer, long fromIndex, long toIndex) {
        try {
            long firstIndex = firstIndex();
            writer.append("# firstIndex: ").append(Long.toHexString(firstIndex)).append("\n");
            try (ExcerptTailer tailer = createTailer()) {
                if (!tailer.moveToIndex(fromIndex)) {
                    if (firstIndex > fromIndex) {
                        tailer.toStart();
                    } else {
                        return;
                    }
                }
                try (ScopedResource<Bytes<Void>> stlBytes = acquireBytesScoped()) {
                    Bytes<?> bytes = stlBytes.get();
                    TextWire text = new TextWire(bytes);
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
                                wire.copyTo(text);
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
                writer.flush();
            } catch (IOException e) {
                Jvm.debug().on(SingleChronicleQueue.class, e);
            }
        }
    }

    // Used in testing.
    public long chunkCount() {
        return chunkCount[0];
    }

    @Override
    public int indexCount() {
        return indexCount;
    }

    @Override
    public int indexSpacing() {
        return indexSpacing;
    }

    @Override
    public long epoch() {
        return epoch;
    }

    @Override
    @NotNull
    public RollCycle rollCycle() {
        return this.rollCycle;
    }

    @SuppressWarnings("deprecation")
    @Override
    public int deltaCheckpointInterval() {
        return 64;
    }

    /**
     * @return if we uses async mode to buffer the appends, the Excerpts are written to the Chronicle Queue using a background thread
     */
    public boolean buffered() {
        return this.isBuffered;
    }

    @NotNull
    public EventLoop eventLoop() {
        return this.eventLoop;
    }

    /**
     * Construct a new {@link ExcerptAppender} once the {@link #createAppenderCondition} is met.
     * @return The new appender
     */
    @NotNull
    protected ExcerptAppender createNewAppenderOnceConditionIsMet() {
        try {
            createAppenderCondition.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedRuntimeException("Interrupted waiting for condition to create appender", e);
        }
        return constructAppender();
    }

    /**
     * Construct a new {@link ExcerptAppender}.
     * <p>
     * This is protected so sub-classes can override the creation of an appender,
     * to create a new appender, sub-classes should call {@link #createNewAppenderOnceConditionIsMet()}
     *
     * @return The new appender
     */
    @NotNull
    protected ExcerptAppender constructAppender() {
        final WireStorePool newPool = WireStorePool.withSupplier(storeSupplier, storeFileListener);
        return new StoreAppender(this, newPool, checkInterrupts);
    }

    protected StoreFileListener storeFileListener() {
        return storeFileListener;
    }

    // used by enterprise CQ
    WireStoreSupplier storeSupplier() {
        return storeSupplier;
    }

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

    @NotNull
    @Override
    public ExcerptAppender createAppender() {
        throwExceptionIfClosed();

        if (readOnly)
            throw new IllegalStateException("Can't append to a read-only chronicle");

        return createNewAppenderOnceConditionIsMet();
    }

    /**
     * @return the {@link WriteLock} that is used to lock writes to the queue. This is the mechanism used to
     * coordinate writes from multiple threads and processes.
     * <p>This lock should only be held for a short time, as it will block progress of any writers to
     * this queue. The default behaviour of {@link TableStoreWriteLock} is to override the lock after a timeout.
     *
     * <p>This is also used to protect rolling to the next cycle
     */
    @NotNull
    WriteLock writeLock() {
        return writeLock;
    }

    /**
     * @return the {@link WriteLock} that is used to lock appends. This is only used by Queue Enterprise
     * sink replication handlers. See Queue Enterprise docs for more details.
     */
    public WriteLock appendLock() {
        return appendLock;
    }

    @NotNull
    @Override
    public ExcerptTailer createTailer(String id) {
        verifyTailerPreconditions(id);
        IndexUpdater indexUpdater = IndexUpdaterFactory.createIndexUpdater(id, this);

        // refresh the listing before creating the tailer
        directoryListing.refresh(true);

        // create the tailer based on up-to-date information.
        final StoreTailer storeTailer = new StoreTailer(this, pool, indexUpdater);
        storeTailer.singleThreadedCheckReset();
        return storeTailer;
    }

    private void verifyTailerPreconditions(String id) {
        // Preconditions for all tailer types
        throwExceptionIfClosed();

        // Named tailer preconditions
        if (id == null) return;
        if (appendLock.locked() && id.startsWith(REPLICATED_NAMED_TAILER_PREFIX)) {
            throw new NamedTailerNotAvailableException(id, NamedTailerNotAvailableException.Reason.NOT_AVAILABLE_ON_SINK);
        }
    }

    @Override
    @NotNull
    public LongValue indexForId(@NotNull String id) {
        return this.metaStore.doWithExclusiveLock((ts) -> ts.acquireValueFor("index." + id, 0L));
    }

    @NotNull
    public LongValue indexVersionForId(@NotNull String id) {
        return this.metaStore.doWithExclusiveLock((ts) -> ts.acquireValueFor(String.format(INDEX_VERSION_FORMAT, id), -1L));
    }

    @NotNull
    public TableStoreWriteLock versionIndexLockForId(@NotNull String id) {
        return new TableStoreWriteLock(
                metaStore,
                pauserSupplier,
                timeoutMS * 3 / 2,
                String.format(SingleChronicleQueue.INDEX_LOCK_FORMAT, id)
        );
    }

    @NotNull
    @Override
    public ExcerptTailer createTailer() {
        throwExceptionIfClosed();

        return createTailer(null);
    }

    @Nullable
    @Override
    public final SingleChronicleQueueStore storeForCycle(int cycle, final long epoch, boolean createIfAbsent, SingleChronicleQueueStore oldStore) {
        return this.pool.acquire(cycle,
                createIfAbsent ? WireStoreSupplier.CreateStrategy.CREATE : WireStoreSupplier.CreateStrategy.READ_ONLY,
                oldStore);
    }

    @Override
    public int nextCycle(int cycle, @NotNull TailerDirection direction) throws ParseException {
        throwExceptionIfClosed();

        return pool.nextCycle(cycle, direction);
    }

    /**
     * Will give you the number of excerpts between 2 index?s ( as exists on the current file system ). If intermediate chronicle files are removed
     * this will effect the result.
     *
     * @param fromIndex the lower index
     * @param toIndex   the higher index
     * @return will give you the number of excerpts between 2 index?s. It?s not as simple as just subtracting one number from the other.
     * @throws IllegalStateException if we are not able to read the chronicle files
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

            // if the are the same
            if (fromIndex == toIndex)
                return 0;

            long result = 0;

            // some of the sequences maybe at -1 so we will add 1 to the cycle and update the result
            // accordingly
            RollCycle rollCycle = rollCycle();
            long sequenceNotSet = rollCycle.toSequenceNumber(-1);

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
                cycles = listCyclesBetween(lowerCycle, upperCycle);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }

            if (cycles.first() == lowerCycle) {
                // because we are inclusive, for example  if we were at the end, then this
                // is 1 except rather than zero
                long l = tailer.excerptsInCycle(lowerCycle);
                result += (l - lowerSeqNum);
            } else
                throw new IllegalStateException("Cycle not found, lower-cycle=" + Long.toHexString(lowerCycle));

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

    public NavigableSet<Long> listCyclesBetween(int lowerCycle, int upperCycle) {
        throwExceptionIfClosed();

        return pool.listCyclesBetween(lowerCycle, upperCycle);
    }

    public <T> void addCloseListener(Closeable key) {
        synchronized (closers) {
            if (!closers.isEmpty())
                closers.removeIf(Closeable::isClosed);
            closers.add(key);
        }
    }

    @Override
    protected void performClose() {
        synchronized (closers) {
            metaStoreMap.values().forEach(Closeable::closeQuietly);
            metaStoreMap.clear();
            closers.forEach(Closeable::closeQuietly);
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

        // close it if we created it.
        if (eventLoop instanceof OnDemandEventLoop)
            eventLoop.close();
    }

    @SuppressWarnings({"deprecation", "removal"})
    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        warnAndCloseIfNotClosed();
    }

    public final void closeStore(@Nullable SingleChronicleQueueStore store) {
        if (store != null)
            this.pool.closeStore(store);
    }

    @Override
    public final int cycle() {
        return cycleCalculator.currentCycle(rollCycle, time, epoch);
    }

    public final int cycle(TimeProvider timeProvider) {
        return cycleCalculator.currentCycle(rollCycle, timeProvider, epoch);
    }

    @Override
    public long firstIndex() {
        int cycle = firstCycle();
        if (cycle == Integer.MAX_VALUE)
            return Long.MAX_VALUE;

        return rollCycle().toIndex(cycle, 0);
    }

    @Override
    public long lastIndex() {
        // This is a slow implementation that gets a Tailer/DocumentContext to find the last index
        try (final ExcerptTailer tailer = createTailer().direction(BACKWARD).toEnd()) {
            while (true) {
                try (final DocumentContext documentContext = tailer.readingDocument()) {
                    if (documentContext.isPresent()) {
                        if (!documentContext.isMetaData())
                            return documentContext.index();
                    } else {
                        return -1;
                    }
                }
            }
        }
    }

    /**
     * This method creates a tailer and count the number of messages between the start of the queue ( see @link firstIndex() )  and the end.
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

    @Nullable
    String[] getList() {
        return path.list();
    }

    private void setFirstAndLastCycle() {
        long now = time.currentTimeMillis();
        if (now <= directoryListing.lastRefreshTimeMS()) {
            return;
        }

        boolean force = now - directoryListing.lastRefreshTimeMS() >= forceDirectoryListingRefreshIntervalMs;
        directoryListing.refresh(force);
    }

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

    @Override
    public int lastCycle() {
        setFirstAndLastCycle();
        return directoryListing.getMaxCreatedCycle();
    }

    @NotNull
    public Consumer<BytesRingBufferStats> onRingBufferStats() {
        return this.onRingBufferStats;
    }

    public long blockSize() {
        return this.blockSize;
    }

    // *************************************************************************
    //
    // *************************************************************************

    public long overlapSize() {
        return this.overlapSize;
    }

    @NotNull
    @Override
    public WireType wireType() {
        return wireType;
    }

    public long bufferCapacity() {
        return this.bufferCapacity;
    }

    @NotNull
    @PackageLocal
    MappedFile mappedFile(File file) throws FileNotFoundException {
        long chunkSize = OS.pageAlign(blockSize);
        final MappedFile mappedFile = MappedFile.of(file, chunkSize, overlapSize, readOnly);
        mappedFile.syncMode(syncMode);
        return mappedFile;
    }

    boolean isReadOnly() {
        return readOnly;
    }

    @NotNull
    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{" +
                "sourceId=" + sourceId +
                ", file=" + path +
                '}';
    }

    @NotNull
    public TimeProvider time() {
        return time;
    }

    @NotNull
    private ToIntFunction<String> fileNameToCycleFunction() {
        return name -> dateCache.parseCount(name.substring(0, name.length() - SUFFIX.length()));
    }

    void removeCloseListener(final StoreTailer storeTailer) {
        synchronized (closers) {
            closers.remove(storeTailer);
        }
    }

    public TableStore<SCQMeta> metaStore() {
        return metaStore;
    }

    public void tableStorePut(CharSequence key, long index) {
        LongValue longValue = tableStoreAcquire(key, index);
        if (longValue == null) return;
        if (index == Long.MIN_VALUE)
            longValue.setVolatileValue(index);
        else
            longValue.setMaxValue(index);
    }

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
                        metaStoreMap.put(key2, longValue);
                        return longValue;
                    }
                }
            }
            return longValue;
        }
    }

    public long tableStoreGet(CharSequence key) {
        LongValue longValue = tableStoreAcquire(key, Long.MIN_VALUE);
        if (longValue == null) return Long.MIN_VALUE;
        return longValue.getVolatileValue();
    }

    @SuppressWarnings("unchecked")
    private BytesStore<?,Void> asBytes(CharSequence key, Bytes<Void> bytes) {
        return key instanceof BytesStore
                ? ((BytesStore<?, Void>) key)
                : bytes.append(key);
    }

    private static final class CachedCycleTree {
        private final long directoryModCount;
        private final NavigableMap<Long, File> cachedCycleTree;

        CachedCycleTree(final long directoryModCount, final NavigableMap<Long, File> cachedCycleTree) {
            this.directoryModCount = directoryModCount;
            this.cachedCycleTree = cachedCycleTree;
        }
    }

    class StoreSupplier extends AbstractCloseable implements WireStoreSupplier {
        private final AtomicReference<CachedCycleTree> cachedTree = new AtomicReference<>();
        private final ReferenceCountedCache<File, MappedFile, MappedBytes, IOException> mappedFileCache;
        private boolean queuePathExists;

        private StoreSupplier() {
            mappedFileCache = new ReferenceCountedCache<>(
                    MappedBytes::mappedBytes,
                    SingleChronicleQueue.this::mappedFile);
            singleThreadedCheckDisabled(true);
        }

        @Override
        public SingleChronicleQueueStore acquire(int cycle, CreateStrategy createStrategy) {
            throwExceptionIfClosed();

            SingleChronicleQueue that = SingleChronicleQueue.this;
            @NotNull final RollingResourcesCache.Resource dateValue = that
                    .dateCache.resourceFor(cycle);
            MappedBytes mappedBytes = null;
            try {
                File path = dateValue.path;

                directoryListing.refresh(false);
                if (createStrategy != CreateStrategy.CREATE &&
                        (cycle > directoryListing.getMaxCreatedCycle()
                                || cycle < directoryListing.getMinCreatedCycle()
                                || !path.exists())) {
                    return null;
                }

                throwExceptionIfClosed();
                if (createStrategy == CreateStrategy.CREATE && !path.exists() && !dateValue.pathExists)
                    PrecreatedFiles.renamePreCreatedFileToRequiredFile(path);

                dateValue.pathExists = true;

                try {
                    mappedBytes = mappedFileCache.get(path);
                } catch (FileNotFoundException e) {
                    createFile(path);
                    mappedBytes = mappedFileCache.get(path);
                }
                mappedBytes.singleThreadedCheckDisabled(true);
                mappedBytes.chunkCount(chunkCount);

//                pauseUnderload();

                if (SHOULD_CHECK_CYCLE && cycle != rollCycle.current(time, epoch)) {
                    Jvm.warn().on(getClass(), new Exception("Creating cycle which is not the current cycle"));
                }
                queuePathExists = true;
                Wire wire = wireType.apply(mappedBytes);
                wire.pauser(pauserSupplier.get());
                wire.headerNumber(rollCycle.toIndex(cycle, 0));

                SingleChronicleQueueStore wireStore;
                try {
                    if (!readOnly && createStrategy == CreateStrategy.CREATE && wire.writeFirstHeader()) {
                        // implicitly reserves the wireStore for this StoreSupplier
                        wireStore = storeFactory.apply(that, wire);

                        createIndexThenUpdateHeader(wire, cycle, wireStore);
                    } else {
                        try {
                            wire.readFirstHeader(timeoutMS, TimeUnit.MILLISECONDS);
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
                            wireStore = valueIn.typedMarshallable();
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

        // Rename un-acquirable segment file to make sure no data is lost to try and recreate the segment
        private CreateStrategy backupCycleFile(int cycle, File cycleFile) {
            File cycleFileDiscard = new File(cycleFile.getParentFile(),
                    String.format("%s-%d%s", cycleFile.getName(), System.currentTimeMillis(), DISCARD_FILE_SUFFIX));
            boolean success = cycleFile.renameTo(cycleFileDiscard);

            // Back-pressure against renaming same cycle multiple times from single queue
            if (success)
                cycleFileRenamed = cycle;

            Jvm.warn().on(SingleChronicleQueue.class, "Renamed un-acquirable segment file to " +
                    cycleFileDiscard.getAbsolutePath() + ": " + success);

            return success ? CreateStrategy.CREATE : CreateStrategy.READ_ONLY;
        }

        private void createIndexThenUpdateHeader(Wire wire, int cycle, SingleChronicleQueueStore wireStore) {
            // Should very carefully prepare all data structures before publishing initial header
            wire.usePadding(wireStore.dataVersion() > 0);
            wire.padToCacheAlign();
            long headerEndPos = wire.bytes().writePosition();
            wireStore.initIndex(wire);
            wire.updateFirstHeader(headerEndPos);
            wire.bytes().writePosition(SPB_HEADER_SIZE);

            // allow directoryListing to pick up the file immediately
            directoryListing.onFileCreated(path, cycle);
        }

        @Override
        protected void performClose() {
            mappedFileCache.close();
        }

        private void createFile(final File path) {
            try {
                File dir = path.getParentFile();
                if (!dir.exists())
                    dir.mkdirs();

                if (!path.createNewFile()) {
                    Jvm.warn().on(getClass(), "unable to create a file at " + path.getAbsolutePath());
                }
            } catch (IOException ex) {
                Jvm.warn().on(getClass(), "unable to create a file at " + path.getAbsolutePath(), ex);
            }
        }

        /**
         * @return cycleTree for the current directory / parentFile
         */
        @NotNull
        private NavigableMap<Long, File> cycleTree(final boolean force) {

            final File parentFile = path;

            // use pre-calculated result in case where queue dir existed when StoreSupplier was constructed
            if (!queuePathExists && !parentFile.exists())
                throw new IllegalStateException("parentFile=" + parentFile.getName() + " does not exist");

            CachedCycleTree cachedValue = cachedTree.get();
            final long directoryModCount = directoryListing.modCount();
            if (force || (cachedValue == null || directoryModCount == -1 || directoryModCount > cachedValue.directoryModCount)) {

                final RollingResourcesCache dateCache = SingleChronicleQueue.this.dateCache;
                final NavigableMap<Long, File> tree = new TreeMap<>();

                final File[] files = parentFile.listFiles((File file) -> file.getPath().endsWith(SUFFIX));
                if (files != null)
                    for (File file : files)
                        tree.put(dateCache.toLong(file), file);

                cachedValue = new CachedCycleTree(directoryModCount, tree);

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

        @Override
        public int nextCycle(int currentCycle, @NotNull TailerDirection direction) {
            throwExceptionIfClosed();

            if (direction == NONE)
                throw new AssertionError("direction is NONE");
            assert currentCycle >= 0 : "currentCycle=" + Integer.toHexString(currentCycle);
            NavigableMap<Long, File> tree = cycleTree(false);
            final File currentCycleFile = dateCache.resourceFor(currentCycle).path;

            // confirm the current cycle is in the min/max range, delay and refresh
            // a few times if not as this suggests files have been deleted
            directoryListing.refresh(false);
            if (currentCycle > directoryListing.getMaxCreatedCycle() ||
                    currentCycle < directoryListing.getMinCreatedCycle()) {
                for (int i = 0; i < 20; i++) {
                    Jvm.pause(10);
                    directoryListing.refresh(i > 1);
                    if (currentCycle <= directoryListing.getMaxCreatedCycle() &&
                            currentCycle >= directoryListing.getMinCreatedCycle()) {
                        break;
                    }
                }
            }

            // check that the current cycle is in the tree, do a hard refresh and retry if not
            Long key = dateCache.toLong(currentCycleFile);
            File file = tree.get(key);
            if (file == null) {
                tree = cycleTree(true);
                file = tree.get(key);
            }

            // The current cycle is no longer on disk, log an error
            if (file == null) {
                Jvm.error().on(SingleChronicleQueue.class, "The current cycle seems to have been deleted from under the queue, scanning to find the next remaining cycle, currentCycle=" + currentCycleFile);
            }

            switch (direction) {
                case FORWARD:
                    return toCycle(tree.higherEntry(key));
                case BACKWARD:
                    return toCycle(tree.lowerEntry(key));
                default:
                    throw new UnsupportedOperationException("Unsupported Direction");
            }
        }

        private int toCycle(@Nullable Map.Entry<Long, File> entry) {
            if (entry == null || entry.getValue() == null)
                return -1;
            return dateCache.parseCount(fileToText().apply(entry.getValue()));
        }

        /**
         * the cycles between a range, inclusive
         *
         * @param lowerCycle the lower cycle inclusive
         * @param upperCycle the upper cycle inclusive
         * @return the cycles between a range, inclusive
         */
        @Override
        public NavigableSet<Long> cycles(int lowerCycle, int upperCycle) {
            throwExceptionIfClosed();

            final NavigableMap<Long, File> tree = cycleTree(false);
            final Long lowerKey = toKey(lowerCycle, "lowerCycle");
            final Long upperKey = toKey(upperCycle, "upperCycle");
            assert lowerKey != null;
            assert upperKey != null;
            return tree.subMap(lowerKey, true, upperKey, true).navigableKeySet();
        }

        @Override
        public boolean canBeReused(@NotNull SingleChronicleQueueStore store) {
            setFirstAndLastCycle();
            int cycle = store.cycle();
            return !store.isClosed() && cycle >= directoryListing.getMinCreatedCycle() && cycle <= directoryListing.getMaxCreatedCycle();
        }

        private Long toKey(int cyle, String m) {
            final File file = dateCache.resourceFor(cyle).path;
            if (!file.exists())
                throw new IllegalStateException("'file not found' for the " + m + ", file=" + file);
            return dateCache.toLong(file);
        }
    }
}
