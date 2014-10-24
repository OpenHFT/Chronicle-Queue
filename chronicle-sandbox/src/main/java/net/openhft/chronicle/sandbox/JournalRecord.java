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

import net.openhft.lang.io.NativeBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JournalRecord extends NativeBytes {
    private static final Logger LOGGER = LoggerFactory.getLogger(JournalRecord.class);
    // int read write lock
    private static final int READ_WRITE_LOCK = -8;
    // int data size
    static final int DATA_SIZE = -4;
    private static final int HEADER_SIZE = 4 + 4;
    private final int size;

    private JournalRecord(int size) {
        super(NativeBytes.NO_PAGE, 0);
        this.size = size;
    }

    public JournalRecord address(long address) {
        positionAddr = startAddr = address + HEADER_SIZE;
        limitAddr = address + size;
        return this;
    }

    public boolean writeLock(long timeOutMS) {
        // first case is the fast path.
        return compareAndSwapInt(READ_WRITE_LOCK, 0, -1) || writeLock0(timeOutMS);
    }

    private boolean writeLock0(long timeOutMS) {
        // use a decrementing timer as this is more robust to jumps in time e.g. debugging, process stop, hibernation.
        long last = System.currentTimeMillis();
        do {
            if (readInt(READ_WRITE_LOCK) == -1)
                return false; // another thread has this lock.
            if (compareAndSwapInt(READ_WRITE_LOCK, 0, -1))
                return true;
            long now = System.currentTimeMillis();
            if (now != last)
                timeOutMS--;
            last = now;
        } while (timeOutMS > 0);
        LOGGER.warn("Grabbing write lock. count={}",readInt(READ_WRITE_LOCK));
        writeOrderedInt(READ_WRITE_LOCK, -1);
        return true;
    }

    public void writeUnlock(int dataSize) {
        assert dataSize <= size;
        writeInt(dataSize);
        compareAndSwapInt(READ_WRITE_LOCK, -1, 0);

    }

    public void readLock() {

    }
}
