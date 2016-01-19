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

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.threads.EventGroup;
import net.openhft.chronicle.threads.api.EventLoop;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;

public class SingleChronicleQueueBuilder implements ChronicleQueueBuilder {
    private File path;
    private long blockSize;
    private WireType wireType;

    private RollCycle rollCycle;

    private long epoch; // default is 1970-01-01 UTC
    private boolean isBuffered;
    private Consumer<Throwable> onThrowable = Throwable::printStackTrace;
    private EventLoop eventGroup;

    private long bufferCapacity = 2 << 20;

    public SingleChronicleQueueBuilder(@NotNull String path) {
        this(new File(path));
    }

    public SingleChronicleQueueBuilder(File path) {
        this.path = path;
        this.blockSize = 64L << 20;
        this.wireType = WireType.BINARY;
        this.rollCycle = RollCycles.DAYS;
        this.epoch = 0;
    }

    public File path() {
        return this.path;
    }

    public SingleChronicleQueueBuilder blockSize(int blockSize) {
        this.blockSize = blockSize;
        return this;
    }

    public long blockSize() {
        return this.blockSize;
    }

    public SingleChronicleQueueBuilder wireType(WireType wireType) {
        this.wireType = wireType;
        return this;
    }

    public WireType wireType() {
        return this.wireType;
    }

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

    public RollCycle rollCycle() {
        return this.rollCycle;
    }

    @NotNull
    public ChronicleQueue build() throws IOException {
        if (isBuffered && eventGroup == null)
            eventGroup = new EventGroup(true, onThrowable);
        return new SingleChronicleQueue(this.clone());
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

    // *************************************************************************
    // HELPERS
    // *************************************************************************

    public static SingleChronicleQueueBuilder binary(File name) {
        return binary(name.getAbsolutePath());
    }

    public static SingleChronicleQueueBuilder binary(String name) {
        return new SingleChronicleQueueBuilder(name)
                .wireType(WireType.BINARY);
    }

    public static SingleChronicleQueueBuilder text(File name) {
        return text(name.getAbsolutePath());
    }

    public static SingleChronicleQueueBuilder text(String name) {
        return new SingleChronicleQueueBuilder(name)
                .wireType(WireType.TEXT);
    }

    public static SingleChronicleQueueBuilder raw(File name) {
        return raw(name.getAbsolutePath());
    }

    public static SingleChronicleQueueBuilder raw(String name) {
        return new SingleChronicleQueueBuilder(name)
                .wireType(WireType.RAW);
    }

    /**
     * use this to trap exceptions  that came from the other threads
     *
     * @param onThrowable your exception handler
     * @return this
     */
    public SingleChronicleQueueBuilder onThrowable(Consumer<Throwable> onThrowable) {
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

    public EventLoop eventGroup() {
        return eventGroup;
    }

    public SingleChronicleQueueBuilder bufferCapacity(int bufferCapacity) {
        this.bufferCapacity = bufferCapacity;
        return this;
    }
}
