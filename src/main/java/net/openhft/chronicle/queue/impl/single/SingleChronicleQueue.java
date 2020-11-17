/*
 * Copyright 2016-2020 chronicle.software
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

import net.openhft.chronicle.bytes.*;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.analytics.Analytics;
import net.openhft.chronicle.core.annotation.PackageLocal;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.pom.PomPropertiesUtil;
import net.openhft.chronicle.core.threads.CleaningThreadLocal;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.OnDemandEventLoop;
import net.openhft.chronicle.core.threads.ThreadLocalHelper;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.core.util.StringUtils;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.*;
import net.openhft.chronicle.queue.impl.table.SingleTableStore;
import net.openhft.chronicle.threads.DiskSpaceMonitor;
import net.openhft.chronicle.threads.TimingPauser;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.ref.WeakReference;
import java.nio.channels.FileLock;
import java.nio.channels.NonWritableChannelException;
import java.text.ParseException;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.*;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;
import static net.openhft.chronicle.queue.TailerDirection.NONE;
import static net.openhft.chronicle.wire.Wires.*;

public class SingleChronicleQueue extends AbstractCloseable implements RollingChronicleQueue {

    public static final String LIBRARY_NAME = "chronicle-queue";
    public static final String SUFFIX = ".cq4";
    public static final String QUEUE_METADATA_FILE = "metadata" + SingleTableStore.SUFFIX;
    public static final String DISK_SPACE_CHECKER_NAME = DiskSpaceMonitor.DISK_SPACE_CHECKER_NAME;
    static final boolean CHECK_INDEX = Jvm.getBoolean("queue.check.index");

    private static final Logger LOG = LoggerFactory.getLogger(SingleChronicleQueue.class);

    private static final Analytics ANALYTICS = Analytics.acquire(LIBRARY_NAME, PomPropertiesUtil.version(LIBRARY_NAME));

    private static final boolean SHOULD_CHECK_CYCLE = Jvm.getBoolean("chronicle.queue.checkrollcycle");
    @NotNull
    protected final EventLoop eventLoop;
    @NotNull
    protected final TableStore<SCQMeta> metaStore;
    // Uses this.closers as a lock. concurrent read, locking for write.
    private final Map<BytesStore, LongValue> metaStoreMap = new ConcurrentHashMap<>();
    final Supplier<TimingPauser> pauserSupplier;
    final long timeoutMS;
    @NotNull
    final File path;
    final String fileAbsolutePath;
    private final StoreSupplier storeSupplier;
    private final ThreadLocal<WeakReference<StoreTailer>> tlTailer = CleaningThreadLocal.withCleanup(wr -> Closeable.closeQuietly(wr.get()));
    @NotNull
    protected final WireStorePool pool;
    private final long epoch;
    private final boolean isBuffered;
    @NotNull
    private final WireType wireType;
    private final long blockSize, overlapSize;
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
    @NotNull
    private final DirectoryListing directoryListing;
    @NotNull
    private final QueueLock queueLock;
    @NotNull
    private final WriteLock writeLock;
    private final boolean checkInterrupts;
    @NotNull
    private final RollingResourcesCache dateCache;
    private final WriteLock appendLock;
    protected int sourceId;
    long firstAndLastCycleTime = 0;
    int firstCycle = Integer.MAX_VALUE, lastCycle = Integer.MIN_VALUE;
    protected final boolean doubleBuffer;
    private StoreFileListener storeFileListener;
    protected final ThreadLocal<ExcerptAppender> strongExcerptAppenderThreadLocal = CleaningThreadLocal.withCloseQuietly(this::newAppender);
    @NotNull
    private RollCycle rollCycle;
    private int deltaCheckpointInterval;

    protected SingleChronicleQueue(@NotNull final SingleChronicleQueueBuilder builder) {
        try {
            rollCycle = builder.rollCycle();
            cycleCalculator = cycleCalculator(builder.rollTimeZone());
            epoch = builder.epoch();
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
            overlapSize = Math.max(64 << 10, builder.blockSize() / 4);
            eventLoop = builder.eventLoop();
            bufferCapacity = builder.bufferCapacity();
            onRingBufferStats = builder.onRingBufferStats();
            indexCount = builder.indexCount();
            indexSpacing = builder.indexSpacing();
            time = builder.timeProvider();
            pauserSupplier = builder.pauserSupplier();
            // add a 10% random element to make it less likely threads will timeout at the same time.
            timeoutMS = (long) (builder.timeoutMS() * (1 + 0.2 * ThreadLocalRandom.current().nextFloat()));
            storeFactory = builder.storeFactory();
            checkInterrupts = builder.checkInterrupts();
            metaStore = builder.metaStore();
            doubleBuffer = builder.doubleBuffer();
            if (metaStore.readOnly() && !builder.readOnly()) {
                LOG.warn("Forcing queue to be readOnly");
                // need to set this on builder as it is used elsewhere
                builder.readOnly(metaStore.readOnly());
            }
            readOnly = builder.readOnly();

            if (readOnly) {
                this.directoryListing = new FileSystemDirectoryListing(path, fileToCycleFunction());
            } else {
                this.directoryListing = new TableDirectoryListing(metaStore, path.toPath(), fileToCycleFunction(), false);
                directoryListing.init();
            }

            this.directoryListing.refresh(true);
            this.queueLock = builder.queueLock();
            this.writeLock = builder.writeLock();

            // release the write lock if the process is dead
            if (writeLock instanceof TableStoreWriteLock) {
                ((TableStoreWriteLock) writeLock).forceUnlockIfProcessIsDead();
            }

            this.appendLock = builder.appendLock();

            if (readOnly) {
                this.lastIndexReplicated = null;
                this.lastAcknowledgedIndexReplicated = null;
            } else {
                this.lastIndexReplicated = metaStore.doWithExclusiveLock(ts -> ts.acquireValueFor("chronicle.lastIndexReplicated", -1L));
                this.lastAcknowledgedIndexReplicated = metaStore.doWithExclusiveLock(ts -> ts.acquireValueFor("chronicle.lastAcknowledgedIndexReplicated", -1L));
            }

            this.deltaCheckpointInterval = builder.deltaCheckpointInterval();

            sourceId = builder.sourceId();

            final Map<String, String> eventParameters = new HashMap<>();
            eventParameters.put("wire_type", wireType.toString());
            final String rollCycleName = rollCycle.toString();
            if (!rollCycleName.startsWith("TEST"))
                eventParameters.put("roll_cycle", rollCycleName);

            ANALYTICS.onStart(eventParameters);
        } catch (Throwable t) {
            close();
            throw Jvm.rethrow(t);
        }
    }

    protected CycleCalculator cycleCalculator(ZoneId zoneId) {
        return DefaultCycleCalculator.INSTANCE;
    }

    @NotNull
    StoreTailer acquireTailer() {
        return ThreadLocalHelper.getTL(tlTailer, this, q -> new StoreTailer(q, q.pool));
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
        // throwExceptionIfClosed();

        return lastAcknowledgedIndexReplicated == null ? -1 : lastAcknowledgedIndexReplicated.getVolatileValue(-1);
    }

    @Override
    public void lastAcknowledgedIndexReplicated(long newValue) {
        // throwExceptionIfClosed();

        if (lastAcknowledgedIndexReplicated != null)
            lastAcknowledgedIndexReplicated.setMaxValue(newValue);
    }

    @Override
    public void refreshDirectoryListing() {
        throwExceptionIfClosed();

        directoryListing.refresh(true);
        firstCycle = directoryListing.getMinCreatedCycle();
        lastCycle = directoryListing.getMaxCreatedCycle();
    }

    /**
     * when using replication to another host, this is the maxiumum last index that has been sent to any of the remote host(s).
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
        sb.append(metaStore.dump());
        for (int i = firstCycle(), max = lastCycle(); i <= max; i++) {
            try (SingleChronicleQueueStore commonStore = storeForCycle(i, epoch, false, null)) {
                if (commonStore != null)
                    sb.append(commonStore.dump());
            }
        }
        return sb.toString();
    }

    @Override
    public void dump(@NotNull Writer writer, long fromIndex, long toIndex) {
        try {
            long firstIndex = firstIndex();
            writer.append("# firstIndex: ").append(Long.toHexString(firstIndex)).append("\n");
            ExcerptTailer tailer = createTailer();
            if (!tailer.moveToIndex(fromIndex)) {
                if (firstIndex > fromIndex) {
                    tailer.toStart();
                } else {
                    return;
                }
            }
            Bytes bytes = acquireBytes();
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
        } catch (Exception e) {
            e.printStackTrace(new PrintWriter(writer));

        } finally {
            try {
                writer.flush();
            } catch (IOException e) {
                LoggerFactory.getLogger(SingleChronicleQueue.class).debug("", e);
            }
        }
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

    @Override
    public int deltaCheckpointInterval() {
        return deltaCheckpointInterval;
    }

    /**
     * @return if we uses a ring buffer to buffer the appends, the Excerpts are written to the Chronicle Queue using a background thread
     */
    public boolean buffered() {
        return this.isBuffered;
    }

    @NotNull
    public EventLoop eventLoop() {
        return this.eventLoop;
    }

    @NotNull
    protected ExcerptAppender newAppender() {

        queueLock.waitForLock();

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
    @Override
    public ExcerptAppender acquireAppender() {
        throwExceptionIfClosed();
        if (readOnly)
            throw new IllegalStateException("Can't append to a read-only chronicle");

        return strongExcerptAppenderThreadLocal.get();
    }

    @Override
    @NotNull
    public QueueLock queueLock() {
        return queueLock;
    }

    @NotNull
    WriteLock writeLock() {
        return writeLock;
    }

    /**
     * @return {@code true} if appends are locked, sink replication handlers use this mechanism to prevent appends being written to.
     */
    public WriteLock appendLock() {
        return appendLock;
    }

    @NotNull
    @Override
    public ExcerptTailer createTailer(String id) {
        throwExceptionIfClosed();

        LongValue index = id == null
                ? null
                : metaStore.doWithExclusiveLock(ts -> ts.acquireValueFor("index." + id, 0));
        final StoreTailer storeTailer = new StoreTailer(this, pool, index);
        directoryListing.refresh(true);
        storeTailer.clearUsedByThread();
        return storeTailer;
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
        return this.pool.acquire(cycle, epoch, createIfAbsent, oldStore);
    }

    @Override
    public int nextCycle(int cycle, @NotNull TailerDirection direction) throws ParseException {
        throwExceptionIfClosed();

        return pool.nextCycle(cycle, direction);
    }

    public long exceptsPerCycle(int cycle) {
        throwExceptionIfClosed();

        StoreTailer tailer = acquireTailer();
        try {
            long index = rollCycle.toIndex(cycle, 0);
            if (tailer.moveToIndex(index)) {
                return tailer.store.lastSequenceNumber(tailer) + 1;
            } else {
                return -1;
            }
        } catch (StreamCorruptedException e) {
            throw new IllegalStateException(e);
        } finally {
            tailer.releaseStore();
        }
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
    public long countExcerpts(long fromIndex, long toIndex) throws IllegalStateException {
        throwExceptionIfClosed();

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
            long l = exceptsPerCycle(lowerCycle);
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
            long l = exceptsPerCycle(lowerCycle);
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
            long x = exceptsPerCycle(Math.toIntExact(array[i]));
            result += x;
        }

        return result;
    }

    public NavigableSet<Long> listCyclesBetween(int lowerCycle, int upperCycle) throws ParseException {
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

    @SuppressWarnings("unchecked")
    @Override
    protected void performClose() {
        synchronized (closers) {
            metaStoreMap.values().forEach(Closeable::closeQuietly);
            metaStoreMap.clear();
            closers.forEach(Closeable::closeQuietly);
            closers.clear();

            // must be closed after closers.
            closeQuietly(directoryListing,
                    queueLock,
                    lastAcknowledgedIndexReplicated,
                    lastIndexReplicated,
                    writeLock,
                    appendLock,
                    pool,
                    storeSupplier,
                    metaStore);
        }

        // close it if we created it.
        if (eventLoop instanceof OnDemandEventLoop)
            eventLoop.close();
    }

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
        // TODO - as discussed, peter is going find another way to do this as this solution
        // currently breaks tests in chronicle engine - see net.openhft.chronicle.engine.queue.LocalQueueRefTest

        int cycle = firstCycle();
        if (cycle == Integer.MAX_VALUE)
            return Long.MAX_VALUE;

        return rollCycle().toIndex(cycle, 0);
    }

    @Override
    public long entryCount() {
        final ExcerptTailer tailer = createTailer();
        tailer.toEnd();
        long lastIndex = tailer.index();
        if (lastIndex == 0)
            return 0;
        return countExcerpts(firstIndex(), lastIndex);
    }

    @Nullable
    String[] getList() {
        return path.list();
    }

    private void setFirstAndLastCycle() {
        long now = System.currentTimeMillis();
        if (now <= firstAndLastCycleTime) {
            return;
        }

        directoryListing.refresh(now - firstAndLastCycleTime > 60_000);
        firstCycle = directoryListing.getMinCreatedCycle();
        lastCycle = directoryListing.getMaxCreatedCycle();

        firstAndLastCycleTime = now;
    }

    @Override
    public int firstCycle() {
        setFirstAndLastCycle();
        return firstCycle;
    }

    /**
     * allows the appenders to inform the queue that they have rolled
     *
     * @param cycle the cycle the appender has rolled to
     */
    void onRoll(int cycle) {
        if (lastCycle < cycle)
            lastCycle = cycle;
        if (firstCycle > cycle)
            firstCycle = cycle;
    }

    @Override
    public int lastCycle() {
        setFirstAndLastCycle();
        return lastCycle;
    }

    protected int fileToCycle(final File queueFile) {
        return fileToCycleFunction().applyAsInt(queueFile);
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
        long overlapSize = OS.pageAlign(blockSize / 4);
        return MappedFile.of(file, chunkSize, overlapSize, readOnly);
    }

    boolean isReadOnly() {
        return readOnly;
    }

    private int toCycle(@Nullable Map.Entry<Long, File> entry) {
        if (entry == null || entry.getValue() == null)
            return -1;
        return dateCache.parseCount(fileToText().apply(entry.getValue()));
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
    private ToIntFunction<File> fileToCycleFunction() {
        return f -> {
            final String name = f.getName();
            return dateCache.parseCount(name.substring(0, name.length() - SUFFIX.length()));
        };
    }

    void removeCloseListener(final StoreTailer storeTailer) {
        synchronized (closers) {
            closers.remove(storeTailer);
        }
    }

    public TableStore metaStore() {
        return metaStore;
    }

    void cleanupStoreFilesWithNoData() {

        writeLock.lock();

        try {
            int cycle = cycle();
            for (int lastCycle = lastCycle(); lastCycle < cycle && lastCycle >= 0; lastCycle--) {
                try (final SingleChronicleQueueStore store = this.pool.acquire(lastCycle, epoch(), false, null)) {
                    // file not found.
                    if (store == null)
                        break;
                    if (store.writePosition() == 0 && !store.file().delete() && store.file().exists()) {
                        // couldn't delete? Let's try writing EOF
                        // if this blows up we should blow up too so don't catch anything
                        MappedBytes bytes = store.bytes();
                        try {
                            store.writeEOFAndShrink(wireType.apply(bytes), timeoutMS);
                        } finally {
                            bytes.releaseLast();
                        }
                        continue;
                    }
                    break;
                }
            }
            directoryListing.refresh(true);
            firstAndLastCycleTime = 0;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    protected boolean threadSafetyCheck(boolean isUsed) {
        // component is thread safe
        return true;
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
    protected LongValue tableStoreAcquire(CharSequence key, long index) {

        BytesStore keyBytes = asBytes(key);
        LongValue longValue = metaStoreMap.get(keyBytes);
        if (longValue == null) {
            synchronized (closers) {
                longValue = metaStoreMap.get(keyBytes);
                if (longValue == null) {
                    longValue = metaStore.acquireValueFor(key, index);
                    int length = key.length();
                    HeapBytesStore<byte[]> key2 = HeapBytesStore.wrap(new byte[length]);
                    key2.write(0, keyBytes, 0, length);
                    metaStoreMap.put(key2, longValue);
                    return null;
                }
            }
        }
        return longValue;
    }

    public long tableStoreGet(CharSequence key) {
        LongValue longValue = tableStoreAcquire(key, Long.MIN_VALUE);
        if (longValue == null) return Long.MIN_VALUE;
        return longValue.getVolatileValue();
    }

    private BytesStore asBytes(CharSequence key) {
        return key instanceof BytesStore
                ? ((BytesStore) key)
                : ((Bytes<?>) acquireAnotherBytes()).append(key);
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
        }

        @SuppressWarnings("resource")
        @Override
        public SingleChronicleQueueStore acquire(int cycle, boolean createIfAbsent) {
            throwExceptionIfClosed();

            SingleChronicleQueue that = SingleChronicleQueue.this;
            @NotNull final RollingResourcesCache.Resource dateValue = that
                    .dateCache.resourceFor(cycle);
            MappedBytes mappedBytes = null;
            try {
                File path = dateValue.path;

                directoryListing.refresh(false);
                if (!createIfAbsent &&
                        (cycle > directoryListing.getMaxCreatedCycle()
                                || cycle < directoryListing.getMinCreatedCycle()
                                || !path.exists())) {
                    return null;
                }

                if (createIfAbsent)
                    checkDiskSpace(that.path);

                if (createIfAbsent && !path.exists() && !dateValue.pathExists)
                    PrecreatedFiles.renamePreCreatedFileToRequiredFile(path);

                dateValue.pathExists = true;

                try {
                    mappedBytes = mappedFileCache.get(path);
                } catch (FileNotFoundException e) {
                    createFile(path);
                    mappedBytes = mappedFileCache.get(path);
                }

//                pauseUnderload();

                if (SHOULD_CHECK_CYCLE && cycle != rollCycle.current(time, epoch)) {
                    LOG.warn("", new Exception("Creating cycle which is not the current cycle"));
                }
                queuePathExists = true;
                AbstractWire wire = (AbstractWire) wireType.apply(mappedBytes);
                assert wire.startUse();
                wire.pauser(pauserSupplier.get());
                wire.headerNumber(rollCycle.toIndex(cycle, 0) - 1);

                SingleChronicleQueueStore wireStore;
                Bytes<?> bytes = wire.bytes();
                try {
                    if (!readOnly && createIfAbsent && wire.writeFirstHeader()) {
                        // implicitly reserves the wireStore for this StoreSupplier
                        wireStore = storeFactory.apply(that, wire);
                        wire.updateFirstHeader();
                        if (wireStore.dataVersion() > 0)
                            wire.usePadding(true);

                        wireStore.initIndex(wire);
                        // do not allow tailer to see the file until it's header is written
                        directoryListing.onFileCreated(path, cycle);
                        // allow directoryListing to pick up the file immediately
                        firstAndLastCycleTime = 0;
                    } else {
                        try {
                            wire.readFirstHeader(timeoutMS, TimeUnit.MILLISECONDS);
                        } catch (TimeoutException e) {

                            headerRecovery(that, mappedBytes, wire, bytes, cycle);
                            return acquire(cycle, createIfAbsent);
                        }

                        StringBuilder name = acquireStringBuilder();
                        ValueIn valueIn = wire.readEventName(name);
                        if (StringUtils.isEqual(name, MetaDataKeys.header.name())) {
                            try {
                                wireStore = valueIn.typedMarshallable();
                            } catch (Throwable t) {
                                mappedBytes.close();
                                throw t;
                            }

                        } else {
                            throw new StreamCorruptedException("The first message should be the header, was " + name);
                        }
                    }
                } catch (InternalError e) {
                    long pos = Objects.requireNonNull(bytes.bytesStore()).addressForRead(0);
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

        private synchronized void headerRecovery(final SingleChronicleQueue that, final MappedBytes mappedBytes, final AbstractWire wire, final Bytes<?> bytes, final int cycle) throws IOException, TimeoutException {

            try {
                final RandomAccessFile raf = mappedBytes.mappedFile().raf();
                final FileLock fileLock = raf.getChannel().tryLock();
                try {

                    final int header = bytes.readVolatileInt(0);
                    if (isReady(header))
                        return;

                    // we hold the file lock so are going to force recovery
                    if (!bytes.compareAndSwapInt(0, header, 0)) {
                        LOG.warn("failed to recover.");
                        throw new StreamCorruptedException("failed to recover.˚");
                    }

                    if (!wire.writeFirstHeader()) {
                        LOG.warn("failed to recover.");
                        throw new StreamCorruptedException("failed to recover.˚");
                    }

                    try (final SingleChronicleQueueStore wireStore = storeFactory.apply(that, wire)) {
                        wire.updateFirstHeader();
                        if (wireStore.dataVersion() > 0)
                            wire.usePadding(true);
                        wireStore.initIndex(wire);
                        directoryListing.onFileCreated(path, cycle);
                        firstAndLastCycleTime = 0;
                    }
                } finally {
                    fileLock.release();
                }

            } catch (NonWritableChannelException e) {
                // this can occur if the queue is in a read only mode
                throw new TimeoutException();
            }

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

                path.createNewFile();
            } catch (IOException ex) {
                Jvm.warn().on(getClass(), "unable to create a file at " + path.getAbsolutePath(), ex);
            }
        }

        private void checkDiskSpace(@NotNull final File filePath) {
            // This operation can stall for 500 ms or more under load.
            DiskSpaceMonitor.INSTANCE.pollDiskSpace(filePath);
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

                for (File file : files) {
                    tree.put(dateCache.toLong(file), file);
                }

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

            directoryListing.refresh(false);
            if (currentCycle > directoryListing.getMaxCreatedCycle() ||
                    currentCycle < directoryListing.getMinCreatedCycle()) {
                boolean fileFound = false;
                for (int i = 0; i < 20; i++) {
                    Jvm.pause(10);
                    directoryListing.refresh(i > 1);
                    if ((fileFound = (
                            currentCycle <= directoryListing.getMaxCreatedCycle() &&
                                    currentCycle >= directoryListing.getMinCreatedCycle()))) {
                        break;
                    }
                }
                fileFound |= currentCycleFile.exists();

                if (!fileFound) {
                    directoryListing.refresh(true);
                    throw new IllegalStateException(
                            String.format("Expected file to exist for cycle: %d, file: %s.%nminCycle: %d, maxCycle: %d%n" +
                                            "Available files: %s",
                                    currentCycle, currentCycleFile,
                                    directoryListing.getMinCreatedCycle(), directoryListing.getMaxCreatedCycle(),
                                    Arrays.toString(path.list((d, n) -> n.endsWith(SingleChronicleQueue.SUFFIX)))));
                }
            }

            Long key = dateCache.toLong(currentCycleFile);
            File file = tree.get(key);
            // already checked that the file should be on-disk, so if it is null, call cycleTree again with force
            if (file == null) {
                tree = cycleTree(true);
                file = tree.get(key);
            }
            if (file == null) {
                throw new AssertionError("missing currentCycle, file=" + currentCycleFile);
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

        private Long toKey(int cyle, String m) {
            final File file = dateCache.resourceFor(cyle).path;
            if (!file.exists())
                throw new IllegalStateException("'file not found' for the " + m + ", file=" + file);
            return dateCache.toLong(file);
        }

        @Override
        protected boolean threadSafetyCheck(boolean isUsed) {
            // StoreSupplier are thread safe
            return true;
        }
    }
}
