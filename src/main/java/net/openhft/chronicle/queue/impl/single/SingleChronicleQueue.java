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

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.AbstractChronicleQueue;
import net.openhft.chronicle.queue.impl.Excerpts;
import net.openhft.chronicle.queue.impl.WireStore;
import net.openhft.chronicle.queue.impl.WireStorePool;
import net.openhft.chronicle.queue.impl.ringbuffer.BytesRingBuffer;
import net.openhft.chronicle.threads.api.EventLoop;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.WiredBytes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.util.function.Consumer;
import java.util.function.Function;

class SingleChronicleQueue extends AbstractChronicleQueue {

    private static final String SUFFIX = ".cq4";

    static {
        ClassAliasPool.CLASS_ALIASES.addAlias(SingleChronicleQueueStore.class, "WireStore");
    }

    @NotNull
    private final SingleChronicleQueueBuilder builder;
    @NotNull
    private final RollCycle cycle;
    @NotNull
    private final RollDateCache dateCache;
    @NotNull
    private final WireStorePool pool;
    private final boolean bufferedAppends;
    private final long epoch;
    private final EventLoop eventloop;


    SingleChronicleQueue(@NotNull final SingleChronicleQueueBuilder builder) {
        this.cycle = builder.rollCycle();
        this.dateCache = new RollDateCache(this.cycle);
        this.builder = builder;
        this.pool = WireStorePool.withSupplier(this::acquireStore);
        storeForCycle(cycle(), builder.epoch());
        epoch = builder.epoch();
        bufferedAppends = builder.buffered();
        eventloop = builder.eventLoop();

    }

    @Override
    public long epoch() {
        return epoch;
    }

    @NotNull
    @Override
    public ExcerptAppender createAppender() {
        @NotNull final Excerpts.StoreAppender storeAppender = new Excerpts.StoreAppender(this);
        if (bufferedAppends) {
            long ringBufferCapacity = BytesRingBuffer.sizeFor(builder
                    .bufferCapacity());
            return new Excerpts.BufferedAppender(eventloop, storeAppender, ringBufferCapacity);
        } else
            return storeAppender;
    }


    @NotNull
    @Override
    public ExcerptTailer createTailer() throws IOException {
        return new Excerpts.StoreTailer(this);
    }

    @Override
    protected WireStore storeForCycle(long cycle, final long epoch) {
        return this.pool.acquire(cycle, epoch);
    }

    @Override
    protected void release(@NotNull WireStore store) {
        this.pool.release(store);
    }

    @Override
    protected long cycle() {
        return this.cycle.current(builder.epoch());
    }

    //TODO: reduce garbage
    //TODO: add a check on first file, in case it gets deleted
    @Override
    public long firstIndex() {
        final long cycle = firstCycle();
        if (cycle == -1)
            return -1;
        @NotNull final WireStore store = acquireStore(cycle, epoch());
        final long sequenceNumber = store.firstSequenceNumber();
        return ChronicleQueue.index(store.cycle(), sequenceNumber);
    }

    private long firstCycle() {
        long firstCycle = -1;

        @NotNull final String basePath = builder.path().getAbsolutePath();
        @Nullable final File[] files = builder.path().listFiles();

        if (files != null && files.length > 0) {
            long firstDate = Long.MAX_VALUE;
            long date;
            String name;

            for (int i = files.length - 1; i >= 0; i--) {
                try {
                    name = files[i].getAbsolutePath();
                    if (name.endsWith(SUFFIX)) {
                        name = name.substring(basePath.length() + 1);
                        name = name.substring(0, name.indexOf('.'));

                        date = dateCache.parseCount(name);
                        if (firstDate > date) {
                            firstDate = date;
                        }
                    }
                } catch (ParseException ignored) {
                    // ignored
                }
            }

            firstCycle = firstDate;
        }


        if (firstCycle == Long.MAX_VALUE) {
            return -1;
        }

        return firstCycle;
    }

    //TODO: reduce garbage


    @Override
    public long lastIndex() {
        final long lastCycle = lastCycle();
        if (lastCycle == -1)
            return -1;
        final long lastSequenceNumber = acquireStore(lastCycle, epoch()).sequenceNumber();
        return ChronicleQueue.index(lastCycle, lastSequenceNumber);
    }

    private long lastCycle() {
        @NotNull final String basePath = builder.path().getAbsolutePath();
        @Nullable final File[] files = builder.path().listFiles();

        if (files != null && files.length > 0) {
            long lastDate = Long.MIN_VALUE;
            long date;
            String name;

            for (int i = files.length - 1; i >= 0; i--) {
                try {
                    name = files[i].getAbsolutePath();
                    if (name.endsWith(SUFFIX)) {
                        name = name.substring(basePath.length() + 1);
                        name = name.substring(0, name.indexOf('.'));

                        date = dateCache.parseCount(name);
                        if (lastDate < date) {
                            lastDate = date;
                        }
                    }
                } catch (ParseException ignored) {
                    // ignored
                }
            }

            return lastDate;
        }

        return -1;
    }


    @Override
    public WireType wireType() {
        return builder.wireType();
    }

    // *************************************************************************
    //
    // *************************************************************************

    @NotNull
    private WireStore acquireStore(final long cycle, final long epoch) {

        final String cycleFormat = this.dateCache.formatFor(cycle);
        @NotNull final File cycleFile = new File(this.builder.path(), cycleFormat + SUFFIX);

        @NotNull final Function<File, MappedBytes> toMappedBytes = file -> {
            try {
                long chunkSize = OS.pageAlign(SingleChronicleQueue.this.builder.blockSize());
                long overlapSize = OS.pageAlign(SingleChronicleQueue.this.builder.blockSize() / 4);
                return MappedBytes.mappedBytes(file,
                        chunkSize,
                        overlapSize);

            } catch (FileNotFoundException e) {
                throw Jvm.rethrow(e);
            }
        };

        if (cycleFile.exists()) {
            final MappedBytes bytes = toMappedBytes.apply(cycleFile);

            final Wire wire = SingleChronicleQueue.this.builder.wireType().apply(bytes);

            try (DocumentContext context = wire.readingDocument()) {
                if (context.isPresent() && context.isMetaData())
                    //noinspection ConstantConditions
                    return wire.getValueIn().typedMarshallable();
            }

        }

        final File parentFile = cycleFile.getParentFile();
        if (parentFile != null && !parentFile.exists()) {
            parentFile.mkdirs();
        }

        @NotNull Consumer<WiredBytes<WireStore>> consumer = ws -> ws.delegate().install(
                ws.headerLength(),
                ws.headerCreated(),
                cycle,
                builder
        );

        @NotNull final Function<MappedBytes, WireStore> supplyStore = mappedBytes -> new
                SingleChronicleQueueStore
                (SingleChronicleQueue.this.builder.rollCycle(), SingleChronicleQueue.this
                        .builder.wireType(), mappedBytes, epoch);

        return WiredBytes.build(
                cycleFile,
                toMappedBytes,
                builder.wireType(),
                supplyStore,
                consumer
        ).delegate();

    }

    @NotNull
    private Excerpts.StoreTailer excerptTailer() {
        try {
            return new Excerpts.StoreTailer(this);
        } catch (IOException e) {
            throw Jvm.rethrow(e);
        }
    }

}
