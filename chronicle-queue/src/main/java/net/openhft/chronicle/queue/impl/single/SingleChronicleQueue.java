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
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollDateCache;
import net.openhft.chronicle.queue.impl.AbstractChronicleQueue;
import net.openhft.chronicle.queue.impl.WireStore;
import net.openhft.chronicle.queue.impl.WireStorePool;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.WiredFile;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;

class SingleChronicleQueue extends AbstractChronicleQueue {
    static {
        ClassAliasPool.CLASS_ALIASES.addAlias(SingleChronicleQueueStore.class,"WireStore");
    }

    private final SingleChronicleQueueBuilder builder;
    private final RollCycle cycle;
    private final RollDateCache dateCache;
    private final WireStorePool pool;
    private int firstCycle;

    protected SingleChronicleQueue(final SingleChronicleQueueBuilder builder) throws IOException {
        this.cycle = builder.rollCycle();
        this.dateCache = new RollDateCache(this.cycle);
        this.builder = builder;
        this.pool = WireStorePool.withSupplier(this::newStore);
        this.firstCycle = -1;
    }

    @Override
    protected synchronized WireStore storeForCycle(long cycle) throws IOException {
        return this.pool.acquire(cycle);
    }

    @Override
    protected synchronized void release(@NotNull WireStore store) {
        this.pool.release(store);
    }

    @Override
    protected long cycle() {
        return this.cycle.current();
    }

    //TODO: reduce garbage
    //TODO: add a check on first file, in case it gets deleted
    @Override
    protected synchronized long firstCycle() {
        if (-1 != firstCycle) {
            return firstCycle;
        }

        final String basePath = builder.path().getAbsolutePath();
        final File[] files = builder.path().listFiles();

        if (files != null) {
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

            firstCycle = (int) firstDate;
        }

        return firstCycle;
    }

    //TODO: reduce garbage
    @Override
    protected long lastCycle() {
        final String basePath = builder.path().getAbsolutePath();
        final File[] files = builder.path().listFiles();

        if (files != null) {
            long lastDate = Long.MIN_VALUE;
            long date = -1;
            String name = null;

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
       throw new UnsupportedOperationException("todo");
    }

    @Override
    public long indexToIndex() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public Wire wire() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public long newIndex() {
        throw new UnsupportedOperationException("todo");
    }

    // *************************************************************************
    //
    // *************************************************************************

    protected WireStore newStore(final long cycle) {
        try {

            String cycleFormat = this.dateCache.formatFor(cycle);
            File cycleFile = new File(this.builder.path(), cycleFormat + ".chronicle");

            if (!cycleFile.getParentFile().exists()) {
                cycleFile.mkdirs();
            }

            return WiredFile.<WireStore>build(
                    cycleFile,
                    file -> MappedFile.mappedFile(file, builder.blockSize()),
                    builder.wireType(),
                    () -> new SingleChronicleQueueStore(builder.rollCycle()),
                    ws -> ws.delegate().install(
                            ws.headerStore(),
                            ws.headerLength(),
                            ws.headerCreated(),
                            cycle,
                            ws.wireSupplier(),
                            ws.mappedFile()
                    )
            ).delegate();

        } catch (IOException e) {
            //TODO: right way ?
            throw new RuntimeException(e);
        }
    }


}
