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

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.text.ParseException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

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
import net.openhft.chronicle.wire.AbstractWire;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
    }

    @Override
    public int sourceId() {
        return sourceId;
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
            sb.append(storeForCycle(i, epoch).dump());
        }
        return sb.toString();
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
    public ExcerptAppender createAppender() {
        return excerptAppenderThreadLocal.get();
    }

    @NotNull
    @Override
    public ExcerptTailer createTailer() {
        return new SingleChronicleQueueExcerpts.StoreTailer(this);
    }

    @NotNull
    @Override
    public final WireStore storeForCycle(int cycle, final long epoch) {
        return this.pool.acquire(cycle, epoch);
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

    public int firstCycle() {
        int firstCycle = Integer.MAX_VALUE;

        @Nullable final String[] files = path.list();

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

            } catch (ParseException ignored) {
                // ignored
            }
        }
        return firstCycle;
    }

    @Override
    public long lastIndex() {
        for (int i = 0; i < 100; i++) {
            try {

                final int lastCycle = lastCycle();
                if (lastCycle == Integer.MIN_VALUE)
                    return Long.MIN_VALUE;

                WireStore store = storeForCycle(lastCycle, epoch);
                Wire wire = wireType().apply(store.bytes());
                long sequenceNumber = store.indexForPosition(wire, store.writePosition(), 0);
                if (sequenceNumber == -1)
                    sequenceNumber++;
                return rollCycle.toIndex(lastCycle, sequenceNumber);
            } catch (EOFException theySeeMeRolling) {
                // continue;
            } catch (TimeoutException e) {
                throw new AssertionError(e);
            }
        }
        throw new IllegalStateException();
    }

    public int lastCycle() {
        int lastCycle = Integer.MIN_VALUE;

        @Nullable final String[] files = path.list();

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

            } catch (ParseException ignored) {
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

    @NotNull
    private WireStore acquireStore(final int cycle, final long epoch) {
        @NotNull final RollingResourcesCache.Resource dateValue = this.dateCache.resourceFor(cycle);
        try {
            final File parentFile = dateValue.path.getParentFile();
            if (parentFile != null && !parentFile.exists())
                parentFile.mkdirs();

            final MappedBytes mappedBytes = mappedBytes(dateValue.path);
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
