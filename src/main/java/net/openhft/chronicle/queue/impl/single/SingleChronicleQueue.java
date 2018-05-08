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
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.ThreadLocalHelper;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.core.util.StringUtils;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.*;
import net.openhft.chronicle.queue.impl.table.SingleTableBuilder;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.ref.WeakReference;
import java.lang.reflect.Method;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.*;

import static net.openhft.chronicle.queue.TailerDirection.NONE;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueExcerpts.StoreAppender;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueExcerpts.StoreTailer;

public class SingleChronicleQueue implements RollingChronicleQueue {

    public static final String SUFFIX = ".cq4";
    public static final String DISK_SPACE_CHECKER_NAME = "disk-space-checker";
    private static ExecutorService DISK_SPACE_CHECKER = null;

    /**
     * Sets the `ExecutorService` that will be used to check disk space. If the executor has
     * already been initialized, this method has no effect.
     */
    public static void initializeDiskSpaceCheckerExecutorService(@NotNull final ExecutorService executorService) {
        if (DISK_SPACE_CHECKER == null) {
            synchronized (SingleChronicleQueue.class) {
                if (DISK_SPACE_CHECKER == null) {
                    DISK_SPACE_CHECKER = executorService;
                }
            }
        }
    }

    @NotNull
    private static ExecutorService getDiskSpaceCheckerExecutorService() {
        if (DISK_SPACE_CHECKER == null) {
            synchronized (SingleChronicleQueue.class) {
                if (DISK_SPACE_CHECKER == null) {
                    DISK_SPACE_CHECKER = Executors.newSingleThreadExecutor(new NamedThreadFactory(DISK_SPACE_CHECKER_NAME, true));
                }
            }
        }

        return DISK_SPACE_CHECKER;
    }

    private static final boolean SHOULD_RELEASE_RESOURCES =
            Boolean.valueOf(System.getProperty("chronicle.queue.release.weakRef.resources",
                    Boolean.TRUE.toString()));
    private static final Logger LOG = LoggerFactory.getLogger(SingleChronicleQueue.class);
    private static final int FIRST_AND_LAST_RETRY_MAX = Integer.getInteger("cq.firstAndLastRetryMax", 1);
    protected final ThreadLocal<WeakReference<ExcerptAppender>> excerptAppenderThreadLocal = new ThreadLocal<>();
    final Supplier<Pauser> pauserSupplier;
    final long timeoutMS;
    @NotNull
    final File path;
    final String fileAbsolutePath;
    final AtomicBoolean isClosed = new AtomicBoolean();
    private final StoreFileListener storeFileListener;
    private final StoreSupplier storeSupplier;
    private final ThreadLocal<WeakReference<StoreTailer>> tlTailer = new ThreadLocal<>();
    @NotNull
    private final WireStorePool pool;
    private final long epoch;
    private final boolean isBuffered;
    @NotNull
    private final WireType wireType;
    private final long blockSize, overlapSize;
    @NotNull
    private final Consumer<BytesRingBufferStats> onRingBufferStats;
    @Nullable
    private final EventLoop eventLoop;
    private final long bufferCapacity;
    private final int indexSpacing;
    private final int indexCount;
    @NotNull
    private final TimeProvider time;
    @NotNull
    private final BiFunction<RollingChronicleQueue, Wire, WireStore> storeFactory;
    private final StoreRecoveryFactory recoverySupplier;
    private final Map<Object, Consumer> closers = new WeakHashMap<>();
    private final boolean readOnly;
    @NotNull
    private final CycleCalculator cycleCalculator;
    @NotNull
    private final Function<String, File> nameToFile;
    @NotNull
    private final DirectoryListing directoryListing;
    @NotNull
    private final QueueLock queueLock;
    private final boolean progressOnContention;
    protected int sourceId;
    long firstAndLastCycleTime = 0;
    int firstAndLastRetry = 0;
    int firstCycle = Integer.MAX_VALUE, lastCycle = Integer.MIN_VALUE;
    @NotNull
    private RollCycle rollCycle;
    @NotNull
    private RollingResourcesCache dateCache;
    private int deltaCheckpointInterval;
    private boolean persistedRollCycleCheckPerformed = false;

    protected SingleChronicleQueue(@NotNull final SingleChronicleQueueBuilder<?> builder) {
        readOnly = builder.readOnly();
        rollCycle = builder.rollCycle();
        cycleCalculator = builder.cycleCalculator();
        epoch = builder.epoch();
        nameToFile = textToFile(builder);
        assignRollCycleDependentFields();

        storeFileListener = builder.storeFileListener();
        storeSupplier = new StoreSupplier();
        pool = WireStorePool.withSupplier(storeSupplier, storeFileListener);
        isBuffered = builder.buffered();
        path = builder.path();
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
        if (readOnly) {
            this.directoryListing = new FileSystemDirectoryListing(path, fileToCycleFunction());
        } else {
            final File listingPath = createDirectoryListingFile();
            this.directoryListing = new TableDirectoryListing(SingleTableBuilder.
                    binary(listingPath).readOnly(builder.readOnly()).build(),
                    path.toPath(), fileToCycleFunction(), builder.readOnly());
            directoryListing.init();
        }

        this.directoryListing.refresh();

        this.queueLock = builder.queueLock();
        addCloseListener(directoryListing, DirectoryListing::close);

        if (builder.getClass().getName().equals("software.chronicle.enterprise.queue.EnterpriseChronicleQueueBuilder")) {
            try {
                Method deltaCheckpointInterval = builder.getClass().getDeclaredMethod
                        ("deltaCheckpointInterval");
                deltaCheckpointInterval.setAccessible(true);
                this.deltaCheckpointInterval = (Integer) deltaCheckpointInterval.invoke(builder);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        sourceId = builder.sourceId();
        recoverySupplier = builder.recoverySupplier();
        progressOnContention = builder.progressOnContention();
    }

    @NotNull
    public static File directoryListingPath(final File queueFolder) throws IOException {
        final File listingPath;
        if ("".equals(queueFolder.getPath())) {
            listingPath = new File(DirectoryListing.DIRECTORY_LISTING_FILE);
        } else {
            listingPath = new File(queueFolder, DirectoryListing.DIRECTORY_LISTING_FILE);
            Path dir = Paths.get(listingPath.getAbsoluteFile().getParent());
            IOTools.createDirectories(dir);
        }
        return listingPath;
    }

    @NotNull
    StoreTailer acquireTailer() {
        if (SHOULD_RELEASE_RESOURCES) {
            return ThreadLocalHelper.getTL(tlTailer, this, StoreTailer::new,
                    StoreComponentReferenceHandler.tailerQueue(),
                    (ref) -> StoreComponentReferenceHandler.register(ref, ref.get().getCloserJob()));
        }
        return ThreadLocalHelper.getTL(tlTailer, this, StoreTailer::new);
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

    @NotNull
    @Override
    public String dump() {
        StringBuilder sb = new StringBuilder();
        for (int i = firstCycle(), max = lastCycle(); i <= max; i++) {
            CommonStore commonStore = storeForCycle(i, epoch, false);
            if (commonStore != null) {
                try {
//                    sb.append("# ").append(wireStore.bytes().mappedFile().file()).append("\n");
                    sb.append(commonStore.dump());
                } finally {
                    release(commonStore);
                }
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
            Bytes bytes = Wires.acquireBytes();
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
    public StoreRecoveryFactory recoverySupplier() {
        return recoverySupplier;
    }

    @Override
    public int deltaCheckpointInterval() {
        return deltaCheckpointInterval;
    }

    /**
     * @return if we uses a ring buffer to buffer the appends, the Excerpts are written to the
     * Chronicle Queue using a background thread
     */
    public boolean buffered() {
        return this.isBuffered;
    }

    @Nullable
    public EventLoop eventLoop() {
        return this.eventLoop;
    }

    @NotNull
    protected ExcerptAppender newAppender() {
        final WireStorePool newPool = WireStorePool.withSupplier(storeSupplier, storeFileListener);
        return new StoreAppender(this, progressOnContention, newPool);
    }

    StoreFileListener storeFileListener() {
        return storeFileListener;
    }

    // used by enterprise CQ
    WireStoreSupplier storeSupplier() {
        return storeSupplier;
    }

    @NotNull
    @Override
    public ExcerptAppender acquireAppender() {
        if (readOnly) {
            throw new IllegalStateException("Can't append to a read-only chronicle");
        }

        queueLock.checkLock();

        if (SHOULD_RELEASE_RESOURCES) {
            return ThreadLocalHelper.getTL(excerptAppenderThreadLocal, this, SingleChronicleQueue::newAppender,
                    StoreComponentReferenceHandler.appenderQueue(),
                    (ref) -> StoreComponentReferenceHandler.register(ref, ref.get().getCloserJob()));
        }

        return ThreadLocalHelper.getTL(excerptAppenderThreadLocal, this, SingleChronicleQueue::newAppender);
    }

    @Override
    @NotNull
    public QueueLock queueLock() {
        return queueLock;
    }

    @NotNull
    @Override
    public ExcerptTailer createTailer() {
        final StoreTailer storeTailer = new StoreTailer(this);
        directoryListing.refresh();
        if (SHOULD_RELEASE_RESOURCES) {
            StoreComponentReferenceHandler.register(
                    new WeakReference<>(storeTailer, StoreComponentReferenceHandler.tailerQueue()),
                    storeTailer.getCloserJob());
        }
        return storeTailer.toStart();
    }

    @Nullable
    @Override
    public final WireStore storeForCycle(int cycle, final long epoch, boolean createIfAbsent) {
        return this.pool.acquire(cycle, epoch, createIfAbsent);
    }

    @Override
    public int nextCycle(int cycle, @NotNull TailerDirection direction) throws ParseException {
        return pool.nextCycle(cycle, direction);
    }

    public long exceptsPerCycle(int cycle) {
        StoreTailer tailer = acquireTailer();
        try {
            long index = rollCycle.toIndex(cycle, 0);
            if (tailer.moveToIndex(index)) {
                assert tailer.store != null && tailer.store.refCount() > 0;
                return tailer.store.lastSequenceNumber(tailer) + 1;
            } else {
                return -1;
            }
        } catch (StreamCorruptedException e) {
            throw new IllegalStateException(e);
        } finally {
            tailer.release();
        }
    }

    /**
     * Will give you the number of excerpts between 2 index’s ( as exists on the current file
     * system ). If intermediate chronicle files are removed this will effect the result.
     *
     * @param fromIndex the lower index
     * @param toIndex   the higher index
     * @return will give you the number of excerpts between 2 index’s. It’s not as simple as just
     * subtracting one number from the other.
     * @throws IllegalStateException if we are not able to read the chronicle files
     */
    @Override
    public long countExcerpts(long fromIndex, long toIndex) throws IllegalStateException {
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
        long sequenceNotSet = rollCycle().toSequenceNumber(-1);

        if (rollCycle().toSequenceNumber(fromIndex) == sequenceNotSet) {
            result++;
            fromIndex++;
        }

        if (rollCycle().toSequenceNumber(toIndex) == sequenceNotSet) {
            result--;
            toIndex++;
        }

        int lowerCycle = rollCycle().toCycle(fromIndex);
        int upperCycle = rollCycle().toCycle(toIndex);

        if (lowerCycle == upperCycle)
            return toIndex - fromIndex;

        long upperSeqNum = rollCycle().toSequenceNumber(toIndex);
        long lowerSeqNum = rollCycle().toSequenceNumber(fromIndex);

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

        final Long[] array = cycles.toArray(new Long[cycles.size()]);
        for (int i = 1; i < array.length - 1; i++) {
            long x = exceptsPerCycle(Math.toIntExact(array[i]));
            result += x;
        }

        return result;
    }

    public NavigableSet<Long> listCyclesBetween(int lowerCycle, int upperCycle) throws ParseException {
        return pool.listCyclesBetween(lowerCycle, upperCycle);
    }

    public <T> void addCloseListener(T key, Consumer<T> closer) {
        synchronized (closers) {
            closers.put(key, closer);
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed.get();
    }

    @Override
    public void close() {
        if (isClosed.getAndSet(true))
            return;
        synchronized (closers) {
            closers.forEach((k, v) -> v.accept(k));
            closers.clear();
        }
        this.pool.close();
    }

    @Override
    public final void release(@Nullable CommonStore store) {
        if (store != null)
            this.pool.release(store);
    }

    @Override
    public final int cycle() {
        return cycleCalculator.currentCycle(rollCycle, time, epoch);
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

    /**
     * Counts the number of messages in this queue instance.
     *
     * @return the number of document excerpts
     */
    public long entryCount() {
        final ExcerptTailer tailer = createTailer();
        tailer.toEnd();
        return countExcerpts(firstIndex(), tailer.index());
    }

    @Nullable
    String[] getList() {
        return path.list();
    }

    private void setFirstAndLastCycle() {
        long now = time.currentTimeMillis();
        if (now == firstAndLastCycleTime) {
            if (++firstAndLastRetry > FIRST_AND_LAST_RETRY_MAX)
                return;
        }

        firstCycle = directoryListing.getMinCreatedCycle();
        lastCycle = directoryListing.getMaxCreatedCycle();

        firstAndLastCycleTime = now;
        firstAndLastRetry = 0;
    }

    @NotNull
    private File createDirectoryListingFile() {
        final File listingPath;
        try {
            listingPath = directoryListingPath(this.path);
            if (!readOnly && listingPath.createNewFile()) {
                if (!listingPath.canWrite()) {
                    throw new IllegalStateException("Cannot write to listing file " + path);
                }
            }
        } catch (IOException e) {
            throw new IORuntimeException("Unable to create listing file " + path, e);
        }
        return listingPath;
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

    public long overlapSize() {
        return this.overlapSize;
    }

    // *************************************************************************
    //
    // *************************************************************************

    @NotNull
    @Override
    public WireType wireType() {
        return wireType;
    }

    public long bufferCapacity() {
        return this.bufferCapacity;
    }

    @NotNull
    private MappedFile mappedFile(File file) throws FileNotFoundException {
        long chunkSize = OS.pageAlign(blockSize);
        long overlapSize = OS.pageAlign(blockSize / 4);
        return MappedFile.of(file, chunkSize, overlapSize, readOnly);
    }

    boolean isReadOnly() {
        return readOnly;
    }

    private int toCycle(@Nullable Map.Entry<Long, File> entry) throws ParseException {
        if (entry == null || entry.getValue() == null)
            return -1;
        return dateCache.parseCount(fileToText().apply(entry.getValue()));
    }

    @NotNull
    @Override
    public String toString() {
        return "SingleChronicleQueue{" +
                "sourceId=" + sourceId +
                ", file=" + path +
                '}';
    }

    @NotNull
    public TimeProvider time() {
        return time;
    }

    void ensureThatRollCycleDoesNotConflictWithExistingQueueFiles() {
        if (!persistedRollCycleCheckPerformed) {
            final Optional<RollCycle> existingRollCycle =
                    RollCycleRetriever.getRollCycle(path.toPath(), wireType, blockSize);
            existingRollCycle.ifPresent(rc -> {
                if (rc != rollCycle) {
                    LOG.warn("Queue created with roll-cycle {}, but files on disk use roll-cycle {}. " +
                            "Overriding this queue to use {}", rollCycle, rc, rc);
                    overrideRollCycle(rc);
                }
            });

            persistedRollCycleCheckPerformed = true;
        }
    }

    private void overrideRollCycle(final RollCycle rollCycle) {
        this.rollCycle = rollCycle;
        assignRollCycleDependentFields();
    }

    private void assignRollCycleDependentFields() {
        dateCache = new RollingResourcesCache(this.rollCycle, epoch, nameToFile,
                fileToText());
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

    @Override
    public long lastAcknowledgedIndexReplicated() {
        return ((StoreAppender) acquireAppender()).store()
                .lastAcknowledgedIndexReplicated();
    }

    private static final class CachedCycleTree {
        private final long directoryModCount;
        private final NavigableMap<Long, File> cachedCycleTree;

        CachedCycleTree(final long directoryModCount, final NavigableMap<Long, File> cachedCycleTree) {
            this.directoryModCount = directoryModCount;
            this.cachedCycleTree = cachedCycleTree;
        }
    }

    private class StoreSupplier implements WireStoreSupplier {
        private final AtomicReference<CachedCycleTree> cachedTree = new AtomicReference<>();
        private final ReferenceCountedCache<File, MappedFile, MappedBytes, IOException> mappedFileCache =
                new ReferenceCountedCache<>(MappedBytes::mappedBytes, SingleChronicleQueue.this::mappedFile);
        private boolean queuePathExists;

        @Override
        public WireStore acquire(int cycle, boolean createIfAbsent) {

            SingleChronicleQueue that = SingleChronicleQueue.this;
            @NotNull final RollingResourcesCache.Resource dateValue = that
                    .dateCache.resourceFor(cycle);
            try {
                File path = dateValue.path;

                if (!createIfAbsent &&
                        (cycle > directoryListing.getMaxCreatedCycle()
                                || cycle < directoryListing.getMinCreatedCycle()
                                || !path.exists())) {
                    return null;
                }

                final File parentFile = dateValue.parentPath;

                if (createIfAbsent)
                    checkDiskSpace(that.path);

                if (!dateValue.pathExists && createIfAbsent && !path.exists()) {
                    parentFile.mkdirs();
                    PrecreatedFiles.renamePreCreatedFileToRequiredFile(path);
                    // before we create a new file, we need to ensure previous file has got EOF mark
                    // but only if we are not in the process of normal rolling
                    QueueFiles.writeEOFIfNeeded(path.toPath(), wireType(), blockSize(), timeoutMS, pauserSupplier.get());
                }
                dateValue.pathExists = true;

                final MappedBytes mappedBytes = mappedFileCache.get(path);
                directoryListing.onFileCreated(path, cycle);
                queuePathExists = true;
                AbstractWire wire = (AbstractWire) wireType.apply(mappedBytes);
                assert wire.startUse();
                wire.pauser(pauserSupplier.get());
                wire.headerNumber(rollCycle.toIndex(cycle, 0) - 1);

                WireStore wireStore;
                if ((!readOnly) && wire.writeFirstHeader()) {
                    wireStore = storeFactory.apply(that, wire);
                    wire.updateFirstHeader();
                } else {
                    wire.readFirstHeader(timeoutMS, TimeUnit.MILLISECONDS);

                    StringBuilder name = Wires.acquireStringBuilder();
                    ValueIn valueIn = wire.readEventName(name);
                    if (StringUtils.isEqual(name, MetaDataKeys.header.name())) {
                        wireStore = valueIn.typedMarshallable();
                        int queueInstanceSourceId = SingleChronicleQueue.this.sourceId;
                        int cq4FileSourceId = wireStore.sourceId();

                        assert queueInstanceSourceId == 0 || cq4FileSourceId == 0 || queueInstanceSourceId ==
                                cq4FileSourceId : "inconsistency with the source id's, the " +
                                "cq4FileSourceId=" +
                                cq4FileSourceId + " != " +
                                "queueInstanceSourceId=" + queueInstanceSourceId;

                        if (cq4FileSourceId != 0)
                            SingleChronicleQueue.this.sourceId = cq4FileSourceId;
                    } else {
                        //noinspection unchecked
                        throw new StreamCorruptedException("The first message should be the header, was " + name);
                    }
                }

                return wireStore;

            } catch (@NotNull TimeoutException | IOException e) {
                throw Jvm.rethrow(e);
            }
        }

        private void checkDiskSpace(@NotNull final File filePath) {
            // This operation can stall for 500 ms or more under load.
            getDiskSpaceCheckerExecutorService().submit(() -> {
                long start = System.nanoTime();
                try {
                    Path path = filePath.getAbsoluteFile().toPath();
                    while (path.getNameCount() > 1) {
                        try {
                            // The returned number of unallocated bytes is a hint, but not a guarantee
                            FileStore fileStore = Files.getFileStore(path);
                            long unallocatedBytes = fileStore.getUnallocatedSpace();
                            long totalSpace = fileStore.getTotalSpace();

                            if (unallocatedBytes < totalSpace * .05)
                                LOG.warn("your disk is more than 95% full, warning: chronicle-queue may crash if " +
                                        "it runs out of space.");
                            else if (unallocatedBytes < (100 << 20)) // if less than 10 Megabytes
                                LOG.warn("your disk is almost full, warning: chronicle-queue may crash if it runs out of space.");
                            return;
                        } catch (IOException nsfe) {
                            // if the leaf directory does not exist, go to parent
                            path = path.getParent();
                        }
                    }
                } catch (Throwable e) {
                    LOG.warn("Failed to check disk space for " + filePath, e);

                } finally {
                    double time = (System.nanoTime() - start) / 1000 / 1000.0;
                    if (time > 1)
                        LOG.debug("Took " + time + " ms to check the disk space.");
                }
            });
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
            if (force || (cachedValue == null || directoryModCount > cachedValue.directoryModCount)) {

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
                }
            }

            return cachedValue.cachedCycleTree;
        }

        @Override
        public int nextCycle(int currentCycle, @NotNull TailerDirection direction) throws ParseException {

            if (direction == NONE)
                throw new AssertionError("direction is NONE");
            assert currentCycle >= 0 : "currentCycle=" + Integer.toHexString(currentCycle);
            NavigableMap<Long, File> tree = cycleTree(false);
            final File currentCycleFile = dateCache.resourceFor(currentCycle).path;

            if (currentCycle > directoryListing.getMaxCreatedCycle() ||
                    currentCycle < directoryListing.getMinCreatedCycle()) {
                boolean fileFound = false;
                for (int i = 0; i < 20; i++) {
                    Jvm.pause(10);
                    if ((fileFound = (
                            currentCycle <= directoryListing.getMaxCreatedCycle() &&
                                    currentCycle >= directoryListing.getMinCreatedCycle()))) {
                        break;
                    }
                }
                fileFound |= currentCycleFile.exists();

                if (!fileFound) {
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
    }
}
