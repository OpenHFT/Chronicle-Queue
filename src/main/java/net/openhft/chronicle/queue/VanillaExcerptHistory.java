/*
 *
 *  *     Copyright (C) 2016  higherfrequencytrading.com
 *  *
 *  *     This program is free software: you can redistribute it and/or modify
 *  *     it under the terms of the GNU Lesser General Public License as published by
 *  *     the Free Software Foundation, either version 3 of the License.
 *  *
 *  *     This program is distributed in the hope that it will be useful,
 *  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  *     GNU Lesser General Public License for more details.
 *  *
 *  *     You should have received a copy of the GNU Lesser General Public License
 *  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

/**
 * Created by peter on 27/03/16.
 */
public class VanillaExcerptHistory implements ExcerptHistory {
    public static final int MESSAGE_HISTORY_LENGTH = 20;
    private static final ThreadLocal<ExcerptHistory> THREAD_LOCAL = ThreadLocal.withInitial(VanillaExcerptHistory::new);

    private int sources;
    private byte[] sourceIdArray = new byte[MESSAGE_HISTORY_LENGTH];
    private long[] sourceIndexArray = new long[MESSAGE_HISTORY_LENGTH];
    private int timings;
    private long[] timingsArray = new long[MESSAGE_HISTORY_LENGTH * 2];

    @Override
    public void reset() {
        sources = timings = 0;
    }

    @Override
    public int timings() {
        return timings;
    }

    @Override
    public long timing(int n) {
        return timingsArray[n];
    }

    @Override
    public int sources() {
        return sources;
    }

    @Override
    public int sourceId(int n) {
        return sourceIdArray[n] & 0xFF;
    }

    @Override
    public long sourceIndex(int n) {
        return sourceIndexArray[n];
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IORuntimeException {
        wire.read(() -> "sources").sequence(this, (t, in) -> {
            t.sources = 0;
            while (in.hasNextSequenceItem()) {
                t.addSource(in.int8(), in.int64());
            }
        });
        wire.read(() -> "timings").sequence(this, (t, in) -> {
            t.timings = 0;
            while (in.hasNextSequenceItem()) {
                t.addTiming(in.int64());
            }
        });
        addSource(wire.sourceId(), wire.sourceIndex());
        addTiming(System.nanoTime());
    }

    private void addSource(int id, long index) {
        sourceIdArray[sources] = (byte) id;
        sourceIndexArray[sources++] = index;
    }

    public void addTiming(long l) {
        timingsArray[timings++] = l;
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write("sources").sequence(this, (t, out) -> {
            for (int i = 0; i < t.sources; i++) {
                out.uint8(t.sourceIdArray[i]);
                out.int64(t.sourceIndexArray[i]);
            }
        });
        wire.write("timings").sequence(this, (t, out) -> {
            for (int i = 0; i < t.timings; i++) {
                out.int64(t.timingsArray[i]);
            }
            out.int64(System.nanoTime());
        });
    }

    static ExcerptHistory getThreadLocal() {
        return THREAD_LOCAL.get();
    }

    static void setThreadLocal(ExcerptHistory md) {
        THREAD_LOCAL.set(md);
    }
}
