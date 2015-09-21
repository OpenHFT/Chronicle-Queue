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

import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollDateCache;
import net.openhft.chronicle.queue.impl.AbstractChronicleQueue;
import net.openhft.koloboke.collect.map.hash.HashIntObjMaps;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Map;

class SingleChronicleQueue extends AbstractChronicleQueue {

    private final SingleChronicleQueueBuilder builder;
    private final RollDateCache dateCache;
    private final Map<Integer, SingleChronicleQueueStore> stores;
    private int firstCycle;

    protected SingleChronicleQueue(final SingleChronicleQueueBuilder builder) throws IOException {
        this.dateCache = new RollDateCache(
            builder.rollCycleLength(),
            builder.rollCycleFormat(),
                builder.rollCycleZoneId());

        this.builder = builder;
        this.stores = HashIntObjMaps.newMutableMap();
        this.firstCycle = -1;
    }

    @Override
    public ExcerptAppender createAppender() throws IOException {
        return new SingleChronicleQueueExcerpts.Appender(this);
    }

    @Override
    public ExcerptTailer createTailer() throws IOException {
        return new SingleChronicleQueueExcerpts.Tailer(this);
    }

    SingleChronicleQueueBuilder builder() {
        return this.builder;
    }

    synchronized SingleChronicleQueueStore storeForCycle(int cycle) throws IOException {
        SingleChronicleQueueStore format = stores.get(cycle);
        if(null == format) {
            stores.put(
                cycle,
                format = new SingleChronicleQueueStore(
                    builder,
                    cycle,
                    this.dateCache.formatFor(cycle)).buildHeader()
            );
        } else {
            format.reserve();
        }

        return format;
    }

    synchronized void release(SingleChronicleQueueStore store) {
        store.release();
        if(store.refCount() <= 0) {
            stores.remove(store.cycle());
        }
    }

    int cycle() {
        return (int) (System.currentTimeMillis() / builder.rollCycleLength());
    }

    //TODO: reduce garbage
    synchronized int firstCycle() {
        if(-1 != firstCycle ) {
            return firstCycle;
        }

        final String basePath = builder.path().getAbsolutePath();
        final File[] files = builder.path().listFiles();

        if(files != null) {
            long firstDate = Long.MAX_VALUE;
            long date = -1;
            String name = null;

            for (int i = files.length - 1; i >= 0; i--) {
                try {
                    name = files[i].getAbsolutePath();
                    if(name.endsWith(".chronicle")) {
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

            firstCycle = (int)firstDate;
        }

        return firstCycle;
    }

    //TODO: reduce garbage
    int lastCycle() {
        final String basePath = builder.path().getAbsolutePath();
        final File[] files = builder.path().listFiles();

        if(files != null) {
            long lastDate = Long.MIN_VALUE;
            long date = -1;
            String name = null;

            for (int i = files.length - 1; i >= 0; i--) {
                try {
                    name = files[i].getAbsolutePath();
                    if(name.endsWith(".chronicle")) {
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

            return (int)lastDate;
        }

        return -1;
    }
}
