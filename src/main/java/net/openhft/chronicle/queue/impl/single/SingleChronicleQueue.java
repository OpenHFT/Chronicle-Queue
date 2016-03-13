/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.openhft.chronicle.queue.impl.single;

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
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.text.ParseException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class SingleChronicleQueue implements RollingChronicleQueue {

    public static final int TIMEOUT = 10_000;
    public static final String MESSAGE = "Timed out waiting for the header record to be ready in ";
    public static final String SUFFIX = ".cq4";

    protected final ThreadLocal<ExcerptAppender> excerptAppenderThreadLocal = ThreadLocal.withInitial(this::newAppender);
    final Supplier<Pauser> pauserSupplier;
    final long timeoutMS;
    @NotNull
    private final RollCycle cycle;
    @NotNull
    private final RollingResourcesCache dateCache;
    @NotNull
    private final WireStorePool pool;
    private final long epoch;
    private final boolean isBuffered;
    private final File path;
    private final WireType wireType;
    private final long blockSize;
    private final RollCycle rollCycle;
    private final Consumer<BytesRingBufferStats> onRingBufferStats;
    private final EventLoop eventLoop;
    private final long bufferCapacity;
    private final int indexSpacing;
    private final int indexCount;
    private final TimeProvider time;

    protected SingleChronicleQueue(@NotNull final SingleChronicleQueueBuilder builder) {
        cycle = builder.rollCycle();
        epoch = builder.epoch();
        dateCache = new RollingResourcesCache(this.cycle, epoch, name -> new File(builder.path(), name + SUFFIX));
        pool = WireStorePool.withSupplier(this::acquireStore);
        isBuffered = builder.buffered();
        path = builder.path();
        wireType = builder.wireType();
        blockSize = builder.blockSize();
        rollCycle = builder.rollCycle();
        eventLoop = builder.eventLoop();
        bufferCapacity = builder.bufferCapacity();
        onRingBufferStats = builder.onRingBufferStats();
        indexCount = builder.indexCount();
        indexSpacing = builder.indexSpacing();
        time = builder.timeProvider();
        pauserSupplier = builder.pauserSupplier();
        timeoutMS = builder.timeoutMS();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @NotNull
    public File path() {
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
    public long epoch() {
        return epoch;
    }

    @NotNull
    public RollCycle rollCycle() {
        return this.cycle;
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
    public final WireStore storeForCycle(long cycle, final long epoch) {
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
        return this.cycle.current(time, epoch);
    }

    @Override
    public long firstIndex() {
        // TODO - as discuessed, peter is going find another way to do this as this solution
        // currently breaks tests in chronicle engine - see net.openhft.chronicle.engine.queue.LocalQueueRefTest

        int cycle = firstCycle();
        if (cycle == Integer.MAX_VALUE)
            return Long.MAX_VALUE;

        return RollingChronicleQueue.index(cycle, 0);
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
        final int lastCycle = lastCycle();
        if (lastCycle == Integer.MIN_VALUE)
            return Long.MIN_VALUE;

        ExcerptTailer tailer = createTailer();
        if (tailer instanceof SingleChronicleQueueExcerpts.StoreTailer)
            return ((SingleChronicleQueueExcerpts.StoreTailer) tailer).lastIndex(lastCycle);
        throw new UnsupportedOperationException();
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
        throw new UnsupportedOperationException("todo");
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

    private MappedBytes mappedBytes(File cycleFile)
            throws FileNotFoundException {
        long chunkSize = OS.pageAlign(blockSize);
        long overlapSize = OS.pageAlign(blockSize / 4);
        return MappedBytes.mappedBytes(cycleFile, chunkSize, overlapSize);
    }

    @NotNull
    private WireStore acquireStore(final long cycle, final long epoch) {
        @NotNull final RollingResourcesCache.Resource dateValue = this.dateCache.resourceFor(cycle);
        try {
            final File parentFile = dateValue.path.getParentFile();
            if (parentFile != null && !parentFile.exists())
                parentFile.mkdirs();

            final MappedBytes mappedBytes = mappedBytes(dateValue.path);

            Wire wire = wireType.apply(mappedBytes);
            wire.pauser(pauserSupplier.get());
            if (wire.writeFirstHeader()) {
                final SingleChronicleQueueStore wireStore = new
                        SingleChronicleQueueStore(rollCycle, wireType, mappedBytes, epoch, indexCount, indexSpacing);
                wire.writeEventName(MetaDataKeys.header).typedMarshallable(wireStore);
                wireStore.writePosition(wire.bytes().writePosition());
                wire.updateFirstHeader();
                return wireStore;
            }

            wire.readFirstHeader(timeoutMS, TimeUnit.MILLISECONDS);

            StringBuilder name = Wires.acquireStringBuilder();
            ValueIn valueIn = wire.readEventName(name);
            if (StringUtils.isEqual(name, MetaDataKeys.header.name()))
                return valueIn.typedMarshallable();
            //noinspection unchecked
            throw new StreamCorruptedException("The first message should be the header, was " + name);

        } catch (TimeoutException | IOException e) {
            throw Jvm.rethrow(e);
        }
    }
}
