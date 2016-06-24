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
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.core.util.StringUtils;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.queue.impl.RollingResourcesCache;
import net.openhft.chronicle.queue.impl.WireStore;
import net.openhft.chronicle.queue.impl.WireStorePool;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.ParseException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class SingleChronicleQueue implements RollingChronicleQueue {

    public static final String SUFFIX = ".cq4";

    protected final ThreadLocal<ExcerptAppender> excerptAppenderThreadLocal = ThreadLocal.withInitial(this::newAppender);
    protected final int sourceId;
    final Supplier<Pauser> pauserSupplier;
    final long timeoutMS;
    @NotNull
    private final RollCycle rollCycle;
    @NotNull
    private final RollingResourcesCache dateCache;
    @NotNull
    private final WireStorePool pool;
    private final long epoch;
    private final boolean isBuffered;
    @NotNull
    private final File path;
    @NotNull
    private final WireType wireType;
    private final long blockSize;
    @NotNull
    private final Consumer<BytesRingBufferStats> onRingBufferStats;
    private final EventLoop eventLoop;
    private final long bufferCapacity;
    private final int indexSpacing;
    private final int indexCount;
    @NotNull
    private final TimeProvider time;
    @NotNull
    private final BiFunction<RollingChronicleQueue, Wire, WireStore> storeFactory;
    private final StoreRecoveryFactory recoverySupplier;

    protected SingleChronicleQueue(@NotNull final SingleChronicleQueueBuilder builder) {
        rollCycle = builder.rollCycle();
        epoch = builder.epoch();
        dateCache = new RollingResourcesCache(this.rollCycle, epoch, name -> new File(builder.path(), name + SUFFIX));
        pool = WireStorePool.withSupplier(this::acquireStore);
        isBuffered = builder.buffered();
        path = builder.path();
        wireType = builder.wireType();
        blockSize = builder.blockSize();
        eventLoop = builder.eventLoop();
        bufferCapacity = builder.bufferCapacity();
        onRingBufferStats = builder.onRingBufferStats();
        indexCount = builder.indexCount();
        indexSpacing = builder.indexSpacing();
        time = builder.timeProvider();
        pauserSupplier = builder.pauserSupplier();
        timeoutMS = builder.timeoutMS();
        storeFactory = builder.storeFactory();
        sourceId = builder.sourceId();
        recoverySupplier = builder.recoverySupplier();
    }

    @Override
    public int sourceId() {
        return sourceId;
    }

    @Override
    public RollCycle rollcycle() {
        return rollCycle;
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @NotNull
    public File file() {
        return path;
    }

    @Override
    public String dump() {
        StringBuilder sb = new StringBuilder();
        for (int i = firstCycle(), max = lastCycle(); i <= max; i++) {
            WireStore wireStore = storeForCycle(i, epoch, false);
            if (wireStore != null) {
//                sb.append("# ").append(wireStore.bytes().mappedFile().file()).append("\n");
                sb.append(wireStore.dump());
            }
        }
        return sb.toString();
    }

    @Override
    public void dump(Writer writer, long fromIndex, long toIndex) {
        try {
            long firstIndex = firstIndex();
            writer.append("# firstIndex: ").append(Long.toHexString(firstIndex)).append("\n");
            writer.append("# nextToWrite: ").append(Long.toHexString(nextIndexToWrite())).append("\n");
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

    @NotNull
    public RollCycle rollCycle() {
        return this.rollCycle;
    }

    @Override
    public StoreRecoveryFactory recoverySupplier() {
        return recoverySupplier;
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

    protected ExcerptAppender newAppender() {
        return new SingleChronicleQueueExcerpts.StoreAppender(this);
    }

    @NotNull
    @Override
    public ExcerptAppender acquireAppender() {
        return excerptAppenderThreadLocal.get();
    }

    @NotNull
    @Override
    public ExcerptTailer createTailer() {
        return new SingleChronicleQueueExcerpts.StoreTailer(this);
    }

    @Nullable
    @Override
    public final WireStore storeForCycle(int cycle, final long epoch, boolean createIfAbsent) {
        return this.pool.acquire(cycle, epoch, createIfAbsent);
    }

    @Override
    public void close() {
        this.pool.close();
    }

    @Override
    public final void release(@NotNull WireStore store) {
        this.pool.release(store);
    }

    @Override
    public final int cycle() {
        return this.rollCycle.current(time, epoch);
    }

    @Override
    public long firstIndex() {
        // TODO - as discuessed, peter is going find another way to do this as this solution
        // currently breaks tests in chronicle engine - see net.openhft.chronicle.engine.queue.LocalQueueRefTest

        int cycle = firstCycle();
        if (cycle == Integer.MAX_VALUE)
            return Long.MAX_VALUE;

        return rollCycle().toIndex(cycle, 0);
    }

//    long lastPathListTime = 0;
//    String[] lastPathList = null;

    String[] getList() {
//        final long now = time.currentTimeMillis();
//        if (lastPathListTime + 10 > now) {
//            return lastPathList;
//        }
//        lastPathListTime = now;
//        return lastPathList = path.list();
        return path.list();
    }

    public int firstCycle() {
        int firstCycle = Integer.MAX_VALUE;

        @Nullable final String[] files = getList();

        if (files == null)
            return Integer.MAX_VALUE;

        for (String file : files) {
            try {
                if (!file.endsWith(SUFFIX))
                    continue;

                file = file.substring(0, file.length() - SUFFIX.length());

                int fileCycle = dateCache.parseCount(file);
                if (firstCycle > fileCycle)
                    firstCycle = fileCycle;

            } catch (ParseException fallback) {
                // ignored
            }
        }
        return firstCycle;
    }

    @Override
    public long nextIndexToWrite() {

        final int lastCycle = lastCycle();
        if (lastCycle == Integer.MIN_VALUE)
            return rollCycle.toIndex(rollCycle.current(time, epoch), 0L);

        WireStore store = storeForCycle(lastCycle, epoch, false);
        if (store == null)
            return Long.MIN_VALUE;

        final long position = store.writePosition();
        return indexFromPosition(lastCycle, store, position);
    }

    @Override
    public long indexFromPosition(int cycle, WireStore store, final long position) {

        final Wire wire = wireType().apply(store.bytes());
        long sequenceNumber = 0;
        try {
            sequenceNumber = store.sequenceForPosition(wire, position, 0);
        } catch (EOFException | StreamCorruptedException e) {
            throw new AssertionError(e);
        }

        return rollCycle.toIndex(cycle, sequenceNumber);
    }

    public int lastCycle() {
        int lastCycle = Integer.MIN_VALUE;

        @Nullable final String[] files = getList();

        if (files == null)
            return Integer.MIN_VALUE;

        for (String file : files) {
            try {
                if (!file.endsWith(SUFFIX))
                    continue;

                file = file.substring(0, file.length() - SUFFIX.length());

                int fileCycle = dateCache.parseCount(file);
                if (lastCycle < fileCycle)
                    lastCycle = fileCycle;

            } catch (ParseException fallback) {
                // ignored
            }
        }
        return lastCycle;
    }

    public Consumer<BytesRingBufferStats> onRingBufferStats() {
        return this.onRingBufferStats;
    }

    public long blockSize() {
        return this.blockSize;
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

    private MappedBytes mappedBytes(File cycleFile) throws FileNotFoundException {
        long chunkSize = OS.pageAlign(blockSize);
        long overlapSize = OS.pageAlign(blockSize / 4);
        return MappedBytes.mappedBytes(cycleFile, chunkSize, overlapSize);
    }

    @net.openhft.chronicle.core.annotation.Nullable
    private WireStore acquireStore(final int cycle, final long epoch, boolean createIfAbsent) {
        @NotNull final RollingResourcesCache.Resource dateValue = this.dateCache.resourceFor(cycle);
        try {
            File path = dateValue.path;
            final File parentFile = path.getParentFile();
            if (parentFile != null && !parentFile.exists())
                parentFile.mkdirs();

            if (!path.exists() && !createIfAbsent)
                return null;

            final MappedBytes mappedBytes = mappedBytes(path);
            AbstractWire wire = (AbstractWire) wireType.apply(mappedBytes);
            assert wire.startUse();
            wire.pauser(pauserSupplier.get());
            wire.headerNumber(rollCycle.toIndex(cycle, 0));

            WireStore wireStore;
            if (wire.writeFirstHeader()) {
                wireStore = storeFactory.apply(this, wire);
                wireStore.writePosition(wire.bytes().writePosition());
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

        } catch (TimeoutException | IOException e) {
            throw Jvm.rethrow(e);
        }
    }

    @Override
    public String toString() {
        return "SingleChronicleQueue{" +
                "sourceId=" + sourceId +
                ", path=" + path +
                '}';
    }
}
