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
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.ThreadLocalHelper;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.core.util.StringUtils;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.*;
import net.openhft.chronicle.queue.impl.table.SingleTableBuilder;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.ref.WeakReference;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static net.openhft.chronicle.queue.TailerDirection.NONE;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueExcerpts.StoreAppender;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueExcerpts.StoreTailer;

public class SingleChronicleQueue implements RollingChronicleQueue {

    public static final String SUFFIX = ".cq4";
    private static final boolean SHOULD_RELEASE_RESOURCES =
            Boolean.valueOf(System.getProperty("chronicle.queue.release.weakRef.resources",
                    Boolean.TRUE.toString()));
    private static final Logger LOG = LoggerFactory.getLogger(SingleChronicleQueue.class);
    private static final int FIRST_AND_LAST_RETRY_MAX = Integer.getInteger("cq.firstAndLastRetryMax", 8);
    protected final ThreadLocal<WeakReference<ExcerptAppender>> excerptAppenderThreadLocal = new ThreadLocal<>();
    protected final int sourceId;
    final Supplier<Pauser> pauserSupplier;
    final long timeoutMS;
    @NotNull
    final File path;
    final AtomicBoolean isClosed = new AtomicBoolean();
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
    private RollCycle rollCycle;
    @NotNull
    private RollingResourcesCache dateCache;
    long firstAndLastCycleTime = 0;
    int firstAndLastRetry = 0;
    int firstCycle = Integer.MAX_VALUE, lastCycle = Integer.MIN_VALUE;
    private int deltaCheckpointInterval;
    private boolean persistedRollCycleCheckPerformed = false;

    protected SingleChronicleQueue(@NotNull final SingleChronicleQueueBuilder builder) {
        rollCycle = builder.rollCycle();
        cycleCalculator = builder.cycleCalculator();
        epoch = builder.epoch();
        nameToFile = textToFile(builder);
        assignRollCycleDependentFields();

        pool = WireStorePool.withSupplier(new StoreSupplier(), builder.storeFileListener());
        isBuffered = builder.buffered();
        path = builder.path();
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
        final File listingPath = createDirectoryListingFile();
        this.directoryListing = new DirectoryListing(SingleTableBuilder.
                binary(listingPath).readOnly(builder.readOnly()).build(), path.toPath(), f -> {
            final String name = f.getName();
            return dateCache.parseCount(name.substring(0, name.length() - SUFFIX.length()));
        }, builder.readOnly());
        this.directoryListing.refresh();
        this.addCloseListener(directoryListing, dl -> {
            dl.close();
        });

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
        readOnly = builder.readOnly();
    }

    @Nullable
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
        return new StoreAppender(this);
    }

    @NotNull
    @Override
    public ExcerptAppender acquireAppender() {
        if (readOnly) {
            throw new IllegalStateException("Can't append to a read-only chronicle");
        }

        if (SHOULD_RELEASE_RESOURCES) {
            return ThreadLocalHelper.getTL(excerptAppenderThreadLocal, this, SingleChronicleQueue::newAppender,
                    StoreComponentReferenceHandler.appenderQueue(),
                    (ref) -> StoreComponentReferenceHandler.register(ref, ref.get().getCloserJob()));
        }

        return ThreadLocalHelper.getTL(excerptAppenderThreadLocal, this, SingleChronicleQueue::newAppender);
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
                assert tailer.store.refCount() > 0;
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
        long now = time.currentTimeMillis() + System.currentTimeMillis();
        if (now == firstAndLastCycleTime) {
            if (++firstAndLastRetry > FIRST_AND_LAST_RETRY_MAX)
                return;
            // give it a moment.
            Thread.yield();
        }

        firstCycle = directoryListing.getMinCreatedCycle();
        lastCycle = directoryListing.getMaxCreatedCycle();


        firstAndLastCycleTime = now;
        firstAndLastRetry = 0;
    }

    @NotNull
    private File createDirectoryListingFile() {
        final File listingPath = new File(path, "directory-listing" + SingleTableBuilder.SUFFIX);
        listingPath.getParentFile().mkdirs();
        try {
            if (listingPath.createNewFile()) {
                if (!listingPath.canWrite()) {
                    throw new IllegalStateException("Cannot write to cycle file");
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Unable to create listing file", e);
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

    @NotNull
    @Override
    public WireType wireType() {
        return wireType;
    }

    public long bufferCapacity() {
        return this.bufferCapacity;
    }

    // *************************************************************************
    //
    // *************************************************************************

    @NotNull
    MappedBytes mappedBytes(@NotNull File cycleFile) throws FileNotFoundException {
        long chunkSize = OS.pageAlign(blockSize);
        long overlapSize = OS.pageAlign(blockSize / 4);
        return MappedBytes.mappedBytes(cycleFile, chunkSize, overlapSize, readOnly);
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

    void removeCloseListener(final StoreTailer storeTailer) {
        synchronized (closers) {
            closers.remove(storeTailer);
        }
    }

    private class StoreSupplier implements WireStoreSupplier {
        @Override
        public WireStore acquire(int cycle, boolean createIfAbsent) {

            SingleChronicleQueue that = SingleChronicleQueue.this;
            @NotNull final RollingResourcesCache.Resource dateValue = that
                    .dateCache.resourceFor(cycle);
            try {
                File path = dateValue.path;
                final File parentFile = dateValue.parentPath;
                if (parentFile != null && !parentFile.exists()) {
                    if (createIfAbsent)
                        parentFile.mkdirs();
                    else
                        return null;
                }

                if (!path.exists() && !createIfAbsent)
                    return null;

                if (createIfAbsent)
                    checkDiskSpace(that.path);

                if (createIfAbsent && !path.exists()) {
                    directoryListing.onFileCreated(path, cycle);
                    // before we create a new file, we need to ensure previous file has got EOF mark
                    QueueFiles.writeEOFIfNeeded(that.path.toPath(), wireType(), blockSize(), timeoutMS);
                }

                final MappedBytes mappedBytes = mappedBytes(path);
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

        private void checkDiskSpace(@NotNull final File filePath) throws IOException {
            Path path = filePath.toPath();
            if (path.getFileSystem() != null) {
                // The returned number of unallocated bytes is a hint, but not a guarantee
                long unallocatedBytes = Files.getFileStore(path).getUnallocatedSpace();
                long totalSpace = Files.getFileStore(path).getTotalSpace();

                if (unallocatedBytes < totalSpace * .05)
                    LOG.warn("your disk is more than 95% full, warning: chronicle-queue may crash if " +
                            "it runs out of space.");
                else if (unallocatedBytes < (100 << 20)) // if less than 10 Megabytes
                    LOG.warn("your disk is almost full, warning: chronicle-queue may crash if it runs out of space.");
            }
        }

        /**
         * @return cycleTree for the current directory / parentFile
         * @throws ParseException
         */
        @NotNull
        private NavigableMap<Long, File> cycleTree() {

            final File parentFile = path;

            if (!parentFile.exists())
                throw new IllegalStateException("parentFile=" + parentFile.getName() + " does not exist");

            final RollingResourcesCache dateCache = SingleChronicleQueue.this.dateCache;
            final NavigableMap<Long, File> tree = new TreeMap<>();

            final File[] files = parentFile.listFiles((File file) -> file.getPath().endsWith(SUFFIX));

            for (File file : files) {
                tree.put(dateCache.toLong(file), file);
            }

            return tree;
        }

        @Override
        public int nextCycle(int currentCycle, @NotNull TailerDirection direction) throws ParseException {

            if (direction == NONE)
                throw new AssertionError("direction is NONE");
            assert currentCycle >= 0 : "currentCycle=" + Integer.toHexString(currentCycle);
            final NavigableMap<Long, File> tree = cycleTree();
            final File currentCycleFile = dateCache.resourceFor(currentCycle).path;

            if (!currentCycleFile.exists())
                throw new IllegalStateException("file not exists, currentCycle, " + "file=" + currentCycleFile);

            Long key = dateCache.toLong(currentCycleFile);
            File file = tree.get(key);
            if (file == null)
                throw new AssertionError("missing currentCycle, file=" + currentCycleFile);

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
         * @throws ParseException
         */
        @Override
        public NavigableSet<Long> cycles(int lowerCycle, int upperCycle) throws ParseException {
            final NavigableMap<Long, File> tree = cycleTree();
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
