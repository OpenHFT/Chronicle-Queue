/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.sandbox;

import net.openhft.lang.io.serialization.BytesMarshallerFactory;
import net.openhft.lang.io.serialization.impl.VanillaBytesMarshallerFactory;

import java.util.TimeZone;

/**
 * @author peter.lawrey
 */
public class DailyRollingConfig implements Cloneable {
    private TimeZone timeZone;
    // 128,000 trillion, more than 170 years worth
    private long maxEntriesPerCycle = 1L << 47;
    // the default maximum entry size
    private long maxEntrySize = 1024 * 1024;
    private BytesMarshallerFactory bytesMarshallerFactory;
    // is synchronous flushing the default
    private boolean synchronousWriter = false;
    // should the reader wait for a flush
    private boolean synchronousReader = false;
    // file format to use for each rolled file.
    private String fileFormat = "yyyyMMdd";
    // rolling period
    private int rollingPeriod = 24 * 60 * 60 * 1000;

    private int indexBlockSize = 1024 * 1024, dataBlockSize = 32 * 1024 * 1024;

    public DailyRollingConfig() {
        setTimeZone(TimeZone.getDefault());
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    DailyRollingConfig setTimeZone(TimeZone timeZone) {
//        if (timeZone.observesDaylightTime())
        this.timeZone = timeZone;
        return this;
    }

    public long getMaxEntriesPerCycle() {
        return maxEntriesPerCycle;
    }

    public DailyRollingConfig setMaxEntriesPerCycle(long maxEntriesPerCycle) {
        this.maxEntriesPerCycle = maxEntriesPerCycle;
        return this;
    }

    public long getMaxEntrySize() {
        return maxEntrySize;
    }

    public DailyRollingConfig setMaxEntrySize(long maxEntrySize) {
        this.maxEntrySize = maxEntrySize;
        return this;
    }

    @Override
    protected DailyRollingConfig clone() {
        try {
            return (DailyRollingConfig) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

    public BytesMarshallerFactory getBytesMarshallerFactory() {
        return bytesMarshallerFactory == null ? new VanillaBytesMarshallerFactory() : bytesMarshallerFactory;
    }

    public DailyRollingConfig setBytesMarshallerFactory(BytesMarshallerFactory bytesMarshallerFactory) {
        this.bytesMarshallerFactory = bytesMarshallerFactory;
        return this;
    }

    public boolean isSynchronousWriter() {
        return synchronousWriter;
    }

    public DailyRollingConfig setSynchronousWriter(boolean synchronousWriter) {
        this.synchronousWriter = synchronousWriter;
        return this;
    }

    public boolean isSynchronousReader() {
        return synchronousReader;
    }

    public DailyRollingConfig setSynchronousReader(boolean synchronousReader) {
        this.synchronousReader = synchronousReader;
        return this;
    }

    public String getFileFormat() {
        return fileFormat;
    }

    public DailyRollingConfig setFileFormat(String fileFormat) {
        this.fileFormat = fileFormat;
        return this;
    }

    public int getIndexBlockSize() {
        return indexBlockSize;
    }

    public DailyRollingConfig setIndexBlockSize(int indexBlockSize) {
        this.indexBlockSize = indexBlockSize;
        return this;
    }

    public int getDataBlockSize() {
        return dataBlockSize;
    }

    public DailyRollingConfig setDataBlockSize(int dataBlockSize) {
        this.dataBlockSize = dataBlockSize;
        return this;
    }

    public int getRollingPeriod() {
        return rollingPeriod;
    }

    public DailyRollingConfig setRollingPeriod(int rollingPeriod) {
        this.rollingPeriod = rollingPeriod;
        return this;
    }
}
