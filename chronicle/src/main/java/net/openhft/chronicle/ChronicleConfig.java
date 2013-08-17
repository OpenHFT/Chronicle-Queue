/*
 * Copyright 2013 Peter Lawrey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle;

import net.openhft.lang.Jvm;

import java.nio.ByteOrder;

/**
 * @author peter.lawrey
 */
public class ChronicleConfig implements Cloneable {
    // 16 billion max, or one per day for 11 years.
    public static final ChronicleConfig SMALL = new ChronicleConfig(4 * 1024, 2 * 1024 * 1024, true, 16 * 1024 * 1024);
    // 256 billion max
    public static final ChronicleConfig MEDIUM = new ChronicleConfig(16 * 1024, 16 * 1024 * 1024, false, 64 * 1024 * 1024);
    // 4 trillion max
    public static final ChronicleConfig LARGE = new ChronicleConfig(64 * 1024, 64 * 1024 * 1024, false, 512 * 1024 * 1024);
    // 1 quadrillion max
    public static final ChronicleConfig HUGE = new ChronicleConfig(4 * 1024 * 1024, 256 * 1024 * 1024, false, 512 * 1024 * 1024);
    // maximise overhead for testing purposes
    public static final ChronicleConfig TEST = new ChronicleConfig(1024 * 1024, 4 * 1024, true, 4 * 1024);
    // default used by Chronicle if not specified.
    public static final ChronicleConfig DEFAULT = Jvm.is64Bit() ? MEDIUM : SMALL;
    private int indexFileCapacity;
    private int indexFileExcerpts;
    private boolean minimiseFootprint;
    // optional parameters, turn on for benchmarks.
    private boolean useUnsafe = false;
    private boolean synchronousMode = false;
    private ByteOrder byteOrder = ByteOrder.nativeOrder();
    private int cacheLineSize = 64;
    private int dataBlockSize;
    private int indexBlockSize;

    public ChronicleConfig(int indexFileCapacity, int indexFileExcerpts, boolean minimiseFootprint, int dataBlockSize) {
        this.indexFileCapacity = indexFileCapacity;
        this.indexFileExcerpts = indexFileExcerpts;
        this.minimiseFootprint = minimiseFootprint;
        this.dataBlockSize = dataBlockSize;
        indexBlockSize = Math.max(4096, this.dataBlockSize / 4);
    }

    public void indexFileCapacity(int indexFileCapacity) {
        this.indexFileCapacity = indexFileCapacity;
    }

    public int indexFileCapacity() {
        return indexFileCapacity;
    }

    public void indexFileExcerpts(int indexFileExcerpts) {
        this.indexFileExcerpts = indexFileExcerpts;
    }

    public int indexFileExcerpts() {
        return indexFileExcerpts;
    }

    public void minimiseFootprint(boolean minimiseFootprint) {
        this.minimiseFootprint = minimiseFootprint;
    }

    public boolean minimiseFootprint() {
        return minimiseFootprint;
    }

    public void useUnsafe(boolean useUnsafe) {
        this.useUnsafe = useUnsafe;
    }

    public boolean useUnsafe() {
        return useUnsafe;
    }

    public void synchronousMode(boolean synchronousMode) {
        this.synchronousMode = synchronousMode;
    }

    public boolean synchronousMode() {
        return synchronousMode;
    }

    public void byteOrder(ByteOrder byteOrder) {
        this.byteOrder = byteOrder;
    }

    public ByteOrder byteOrder() {
        return byteOrder;
    }

    public void cacheLineSize(int cacheLineSize) {
        this.cacheLineSize = cacheLineSize;
    }

    public int cacheLineSize() {
        return cacheLineSize;
    }

    public void dataBlockSize(int dataBlockSize) {
        this.dataBlockSize = dataBlockSize;
    }

    public int dataBlockSize() {
        return dataBlockSize;
    }

    public void indexBlockSize(int indexBlockSize) {
        this.indexBlockSize = indexBlockSize;
    }

    public int indexBlockSize() {
        return indexBlockSize;
    }

    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    @Override
    public ChronicleConfig clone() {
        try {
            return (ChronicleConfig) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }
}
