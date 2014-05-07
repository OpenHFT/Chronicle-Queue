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

import java.util.concurrent.TimeUnit;

public class VanillaChronicleConfig {
    public static final VanillaChronicleConfig DEFAULT = new VanillaChronicleConfig();

    public static final long MIN_CYCLE_LENGTH = TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);

    /**
     * Number of most-significant bits used to hold the appender id in index entries.
     * The remaining least-significant bits of the index entry are used for the data offset info.
     */
    public static final int APPENDER_ID_BITS = 24;

    /**
     * Mask used to validate that the appender id does not exceed the allocated number of bits.
     */
    public static final long APPENDER_ID_MASK = -1L >>> -APPENDER_ID_BITS;

    /**
     * Number of least-significant bits used to hold the data offset info in index entries.
     */
    public static final int INDEX_DATA_OFFSET_BITS = 64 - APPENDER_ID_BITS;

    /**
     * Mask used to extract the data offset info from an index entry.
     */
    public static final long INDEX_DATA_OFFSET_MASK = -1L >>> -INDEX_DATA_OFFSET_BITS;


    private String cycleFormat = "yyyyMMdd";
    private int cycleLength = 24 * 60 * 60 * 1000; // MILLIS_PER_DAY
    private long indexBlockSize = 16L << 20; // 16 MB
    private long dataBlockSize = 64L << 20; // 16 MB
    private int defaultMessageSize = 128 << 10; // 128 KB.
    private long entriesPerCycle = 1L << 40; // one trillion per day or per hour.
    private boolean synchronous = false;

    public VanillaChronicleConfig cycleFormat(String cycleFormat) {
        this.cycleFormat = cycleFormat;
        return this;
    }

    public String cycleFormat() {
        return cycleFormat;
    }

    public VanillaChronicleConfig cycleLength(int cycleLength) {
        return cycleLength(cycleLength, true);
    }

    VanillaChronicleConfig cycleLength(int cycleLength, boolean check) {
        if (check && cycleLength < MIN_CYCLE_LENGTH) {
            throw new IllegalArgumentException("Cycle length can't be less than " + MIN_CYCLE_LENGTH + " ms!");
        }
        this.cycleLength = cycleLength;
        return this;
    }

    public int cycleLength() {
        return cycleLength;
    }

    public VanillaChronicleConfig indexBlockSize(long indexBlockSize) {
        this.indexBlockSize = indexBlockSize;
        return this;
    }

    public long indexBlockSize() {
        return indexBlockSize;
    }

    public long dataBlockSize() {
        return dataBlockSize;
    }

    public VanillaChronicleConfig dataBlockSize(int dataBlockSize) {
        this.dataBlockSize = dataBlockSize;
        return this;
    }

    public VanillaChronicleConfig entriesPerCycle(long entriesPerCycle) {
        this.entriesPerCycle = entriesPerCycle;
        return this;
    }

    public long entriesPerCycle() {
        return entriesPerCycle;
    }

    public VanillaChronicleConfig defaultMessageSize(int defaultMessageSize) {
        this.defaultMessageSize = defaultMessageSize;
        return this;
    }

    public int defaultMessageSize() {
        return defaultMessageSize;
    }

    public VanillaChronicleConfig synchronous(boolean synchronous) {
        this.synchronous = synchronous;
        return this;
    }

    public boolean synchronous() {
        return synchronous;
    }


}
