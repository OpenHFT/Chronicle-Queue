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

package net.openhft.chronicle.sandbox.attic;

public class JournalConfig implements Cloneable {
    private int indexCount = 4 * 1024;
    private int dataShift = 8;
    private int dataAllocations = 1 << 20;

    public JournalConfig indexCount(int indexCount) {
        this.indexCount = indexCount;
        return this;
    }

    public int indexCount() {
        return indexCount;
    }

    public JournalConfig dataShift(int dataShift) {
        this.dataShift = dataShift;
        return this;
    }

    public int dataShift() {
        return dataShift;
    }

    public JournalConfig dataAllocations(int dataAllocations) {
        this.dataAllocations = dataAllocations;
        return this;
    }

    public int dataAllocations() {
        return dataAllocations;
    }

    @Override
    protected JournalConfig clone() {
        try {
            return (JournalConfig) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }
}
