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

import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollDateCache;
import net.openhft.chronicle.queue.impl.AbstractChronicleQueue;
import net.openhft.chronicle.queue.impl.Excerpts;
import net.openhft.chronicle.queue.impl.WireStore;
import net.openhft.chronicle.queue.impl.WireStorePool;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.WiredFile;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.util.function.Consumer;
import java.util.function.Function;

class SingleChronicleQueue extends AbstractChronicleQueue {
    static {
        ClassAliasPool.CLASS_ALIASES.addAlias(SingleChronicleQueueStore.class, "WireStore");
    }

    private final SingleChronicleQueueBuilder builder;
    private final RollCycle cycle;
    private final RollDateCache dateCache;
    private final WireStorePool pool;
    private long firstCycle;

    protected SingleChronicleQueue(final SingleChronicleQueueBuilder builder) throws IOException {
        this.cycle = builder.rollCycle();
        this.dateCache = new RollDateCache(this.cycle);
        this.builder = builder;
        this.pool = WireStorePool.withSupplier(this::newStore);
        this.firstCycle = -1;
        this.pool.acquire(cycle());
    }

    @Override
    public ExcerptAppender createAppender() throws IOException {
        return new Excerpts.StoreAppender(this);
    }

    @Override
    public ExcerptTailer createTailer() throws IOException {
        return new Excerpts.StoreTailer(this);
    }

    @Override
    protected WireStore storeForCycle(long cycle) throws IOException {
        return this.pool.acquire(cycle);
    }

    @Override
    protected void release(@NotNull WireStore store) {
        this.pool.release(store);
    }

    @Override
    protected long cycle() {
        return this.cycle.current();
    }

    //TODO: reduce garbage
    //TODO: add a check on first file, in case it gets deleted
    @Override
    protected long firstCycle() {
        if (firstCycle != -1) {
            return firstCycle;
        }

        final String basePath = builder.path().getAbsolutePath();
        final File[] files = builder.path().listFiles();

        if (files != null && files.length > 0) {
            long firstDate = Long.MAX_VALUE;
            long date = -1;
            String name = null;

            for (int i = files.length - 1; i >= 0; i--) {
                try {
                    name = files[i].getAbsolutePath();
                    if (name.endsWith(".chronicle")) {
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

        return firstCycle;
    }

    //TODO: reduce garbage
    @Override
    protected long lastCycle() {
        final String basePath = builder.path().getAbsolutePath();
        final File[] files = builder.path().listFiles();

        if (files != null && files.length > 0) {
            long lastDate = Long.MIN_VALUE;
            long date;
            String name;

            for (int i = files.length - 1; i >= 0; i--) {
                try {
                    name = files[i].getAbsolutePath();
                    if (name.endsWith(".chronicle")) {
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

    protected WireStore newStore(final long cycle) {

        final String cycleFormat = this.dateCache.formatFor(cycle);
        final File cycleFile = new File(this.builder.path(), cycleFormat + ".chronicle");

        File parentFile = cycleFile.getParentFile();
        if (parentFile != null & !parentFile.exists()) {
            parentFile.mkdirs();
        }

        final Function<File, MappedFile> toMappedFile = file -> {
            try {
                return MappedFile.mappedFile(file,
                        SingleChronicleQueue.this.builder.blockSize(),
                        SingleChronicleQueue.this.builder.blockSize());
            } catch (FileNotFoundException e) {
                throw Jvm.rethrow(e);
            }
        };

        Function<MappedFile, WireStore> supplyStore = mappedFile -> new SingleChronicleQueueStore
                (SingleChronicleQueue.this.builder.rollCycle(), SingleChronicleQueue.this
                        .builder.wireType(), mappedFile);

        Consumer<WiredFile<WireStore>> consumer = ws -> {
            try {
                ws.delegate().install(
                        ws.mappedFile(),
                        ws.headerLength(),
                        ws.headerCreated(),
                        cycle,
                        builder,
                        ws.wireSupplier(),
                        ws.mappedFile()
                );
            } catch (IOException e) {
                throw Jvm.rethrow(e);
            }
        };

        return WiredFile.build(
                cycleFile,
                toMappedFile,
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
