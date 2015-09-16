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
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;

public class SingleChronicleQueueBuilder implements ChronicleQueueBuilder {
    private File path;
    private long blockSize;
    private WireType wireType;

    private int headerWaitLoops;
    private int headerWaitDelay;


    private int appendWaitLoops;
    private int appendWaitDelay;

    public SingleChronicleQueueBuilder(String path) {
        this(new File(path));
    }

    public SingleChronicleQueueBuilder(File path) {
        this.path = path;
        this.blockSize = 64L << 20;
        this.wireType = WireType.BINARY;
        this.headerWaitLoops = 1000;
        this.headerWaitDelay = 10;
        this.appendWaitLoops = 1000;
        this.appendWaitDelay = 0;
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

    public SingleChronicleQueueBuilder headerWaitLoops(int headerWaitLoops) {
        this.headerWaitLoops = headerWaitLoops;
        return this;
    }

    public int headerWaitLoops() {
        return this.headerWaitLoops;
    }

    public SingleChronicleQueueBuilder headerWaitDelay(int headerWaitDelay) {
        this.headerWaitDelay = headerWaitDelay;
        return this;
    }

    public int headerWaitDelay() {
        return this.headerWaitDelay;
    }


    public SingleChronicleQueueBuilder appendWaitLoops(int appendWaitLoops) {
        this.appendWaitLoops = appendWaitLoops;
        return this;
    }

    public int appendWaitLoops() {
        return this.appendWaitLoops;
    }

    public SingleChronicleQueueBuilder appendWaitDelay(int appendWaitDelay) {
        this.appendWaitDelay = appendWaitDelay;
        return this;
    }

    public int appendWaitDelay() {
        return this.appendWaitDelay;
    }

    @NotNull
    public ChronicleQueue build() throws IOException {
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
}
