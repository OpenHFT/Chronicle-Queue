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
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.ExcerptFactory;
import net.openhft.chronicle.threads.EventGroup;
import net.openhft.chronicle.threads.api.EventLoop;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.function.Consumer;

public class SingleChronicleQueueBuilder implements ChronicleQueueBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(SingleChronicleQueueBuilder.class.getName());
    private final File path;
    private long blockSize;

    @NotNull
    private WireType wireType;

    @NotNull
    private RollCycle rollCycle;

    private long epoch; // default is 1970-01-01 UTC
    private boolean isBuffered;
    private Consumer<Throwable> onThrowable = Throwable::printStackTrace;

    @Nullable
    private EventLoop eventLoop;

    @NotNull
    private ExcerptFactory<SingleChronicleQueue> excerptFactory;

    private long bufferCapacity = 2 << 20;

    /**
     * by default logs the performance stats of the ring buffer
     */
    private Consumer<BytesRingBufferStats> onRingBufferStats = r -> {

        long writeBytesRemaining = r.minNumberOfWriteBytesRemaining();

        if (writeBytesRemaining == Long.MAX_VALUE)
            return;

        double percentageFree = ((double) writeBytesRemaining / (double) r.capacity()) * 100;

        if (percentageFree > 0.5)
            return;

        final long writeCount = r.getAndClearWriteCount();
        final long readCount = r.getAndClearReadCount();

        LOG.info("ring buffer=" + (r.capacity() - writeBytesRemaining) / 1024 +
                "KB/" + r.capacity() / 1024 + "KB [" + (int) percentageFree + "% Free], " +
                "" + "writes=" + writeCount + ", reads=" + readCount + ", " +
                "maxCopyTimeNs=" + r.maxCopyTimeNs() / 1000 + " us");
    };


    public SingleChronicleQueueBuilder(@NotNull String path) {
        this(new File(path));
    }

    protected SingleChronicleQueueBuilder(@NotNull String path, @NotNull ExcerptFactory<SingleChronicleQueue> excerptFactory) {
        this(new File(path), excerptFactory);
    }

    public SingleChronicleQueueBuilder(@NotNull File path) {
        this(path, SingleChronicleQueueExcerptFactory.INSTANCE);
    }

    protected SingleChronicleQueueBuilder(@NotNull File path, @NotNull ExcerptFactory<SingleChronicleQueue> excerptFactory) {
        this.path = path;
        this.blockSize = 64L << 20;
        this.wireType = WireType.BINARY;
        this.rollCycle = RollCycles.DAYS;
        this.epoch = 0;
        this.excerptFactory = excerptFactory;
    }

    @NotNull
    public static SingleChronicleQueueBuilder binary(@NotNull File name) {
        return binary(name.getAbsolutePath());
    }

    @NotNull
    private static SingleChronicleQueueBuilder binary(@NotNull String name) {
        return new SingleChronicleQueueBuilder(name)
                .wireType(WireType.BINARY);
    }

    @NotNull
    public static SingleChronicleQueueBuilder text(@NotNull File name) {
        return text(name.getAbsolutePath());
    }

    @NotNull
    private static SingleChronicleQueueBuilder text(@NotNull String name) {
        return new SingleChronicleQueueBuilder(name)
                .wireType(WireType.TEXT);
    }

    @NotNull
    public static SingleChronicleQueueBuilder raw(@NotNull File name) {
        return raw(name.getAbsolutePath());
    }

    @NotNull
    private static SingleChronicleQueueBuilder raw(@NotNull String name) {
        return new SingleChronicleQueueBuilder(name)
                .wireType(WireType.RAW);
    }

    /**
     * consumer will be called every second, also as there is data to report
     *
     * @param onRingBufferStats a consumer of the BytesRingBufferStats
     * @return this
     */
    @NotNull
    public SingleChronicleQueueBuilder onRingBufferStats(@NotNull Consumer<BytesRingBufferStats> onRingBufferStats) {
        this.onRingBufferStats = onRingBufferStats;
        return this;
    }

    public Consumer<BytesRingBufferStats> onRingBufferStats() {
        return this.onRingBufferStats;
    }

    @NotNull
    public File path() {
        return this.path;
    }

    @NotNull
    public SingleChronicleQueueBuilder blockSize(int blockSize) {
        this.blockSize = blockSize;
        return this;
    }

    public long blockSize() {
        return this.blockSize;
    }

    @NotNull
    public SingleChronicleQueueBuilder wireType(@NotNull WireType wireType) {
        this.wireType = wireType;
        return this;
    }

    @NotNull
    public WireType wireType() {
        return this.wireType;
    }

    @NotNull
    public SingleChronicleQueueBuilder rollCycle(@NotNull RollCycle rollCycle) {
        this.rollCycle = rollCycle;
        return this;
    }

    /**
     * @return ringBufferCapacity in bytes
     */
    public long bufferCapacity() {
        return bufferCapacity;
    }

    /**
     * @param ringBufferSize sets the ring buffer capacity in bytes
     * @return this
     */
    @NotNull
    public SingleChronicleQueueBuilder bufferCapacity(long ringBufferSize) {
        this.bufferCapacity = ringBufferSize;
        return this;
    }

    /**
     * sets epoch offset in milliseconds
     *
     * @param epoch sets an epoch offset as the number of number of milliseconds since January 1,
     *              1970,  00:00:00 GMT
     * @return {@code this}
     */
    @NotNull
    public SingleChronicleQueueBuilder epoch(long epoch) {
        this.epoch = epoch;
        return this;
    }

    /**
     * @return epoch offset as the number of number of milliseconds since January 1, 1970,  00:00:00
     * GMT
     */
    public long epoch() {
        return epoch;
    }

    @NotNull
    public RollCycle rollCycle() {
        return this.rollCycle;
    }

    @NotNull
    public SingleChronicleQueueBuilder excertpFactory(@NotNull ExcerptFactory<SingleChronicleQueue> excerptFactory) {
        this.excerptFactory = excerptFactory;
        return this;
    }

    public ExcerptFactory<SingleChronicleQueue> excertpFactory() {
        return this.excerptFactory;
    }

    // *************************************************************************
    //
    // *************************************************************************

    @NotNull
    public ChronicleQueue build() {
        if (isBuffered && eventLoop == null)
            eventLoop = new EventGroup(true, onThrowable);

        return new SingleChronicleQueue(clone());
    }

    @NotNull
    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    @Override
    public SingleChronicleQueueBuilder clone() {
        try {
            return (SingleChronicleQueueBuilder) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * use this to trap exceptions  that came from the other threads
     *
     * @param onThrowable your exception handler
     * @return this
     */
    @NotNull
    public SingleChronicleQueueBuilder onThrowable(@NotNull Consumer<Throwable> onThrowable) {
        this.onThrowable = onThrowable;
        return this;
    }


    /**
     * when set to {@code true}. uses a ring buffer to buffer appends, excerpts are written to the
     * Chronicle Queue using a background thread
     *
     * @param isBuffered {@code true} if the append is buffered
     * @return this
     */
    @NotNull
    public SingleChronicleQueueBuilder buffered(boolean isBuffered) {
        this.isBuffered = isBuffered;
        return this;
    }

    /**
     * @return if we uses a ring buffer to buffer the appends, the Excerts are written to the
     * Chronicle Queue using a background thread
     */
    public boolean buffered() {
        return this.isBuffered;
    }

    @Nullable
    public EventLoop eventLoop() {
        return eventLoop;
    }

    @NotNull
    public SingleChronicleQueueBuilder eventLoop(EventLoop eventLoop) {
        this.eventLoop = eventLoop;
        return this;
    }

    /**
     * setting the {@code bufferCapacity} also sets {@code buffered} to {@code true}
     *
     * @param bufferCapacity the capacity of the ring buffer
     * @return this
     */
    @NotNull
    public SingleChronicleQueueBuilder bufferCapacity(int bufferCapacity) {
        this.bufferCapacity = bufferCapacity;
        this.isBuffered = true;
        return this;
    }


}
