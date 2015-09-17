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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

class SingleChronicleQueue extends AbstractChronicleQueue {

    private final SingleChronicleQueueBuilder builder;
    private final RollDateCache dateCache;
    private final Map<Integer, SingleChronicleQueueFormat> formatCache;

    protected SingleChronicleQueue(final SingleChronicleQueueBuilder builder) throws IOException {
        this.dateCache = new RollDateCache(
            builder.rollCycleLength(),
            builder.rollCycleFormat(),
            builder.rollCycleZoneId());

        this.builder = builder;
        this.formatCache = new HashMap<>();
    }

    @Override
    public ExcerptAppender createAppender() {
        return new SingleChronicleQueueExcerpts.Appender(this);
    }

    @Override
    public ExcerptTailer createTailer() {
        return new SingleChronicleQueueExcerpts.Tailer(this);
    }

    SingleChronicleQueueBuilder builder() {
        return this.builder;
    }

    //TODO: maybe use kolobroke ?
    synchronized SingleChronicleQueueFormat formatForCycle(int cycle) throws IOException {
        SingleChronicleQueueFormat format = formatCache.get(cycle);
        if(null == format) {
            formatCache.put(
                cycle,
                format = new SingleChronicleQueueFormat(
                    builder,
                    cycle,
                    this.dateCache.formatFor(cycle)).buildHeader()
            );
        }

        return format;
    }

    int cycle() {
        return (int) (System.currentTimeMillis() / builder.rollCycleLength());
    }
}
