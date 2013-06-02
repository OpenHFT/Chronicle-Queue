/*
 * Copyright ${YEAR} Peter Lawrey
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

import net.openhft.lang.io.NativeBytes;
import sun.nio.ch.DirectBuffer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author peter.lawrey
 */
public class IndexedChronicle implements Chronicle {
    private final FileChannel indexFile, dataFile;
    private final int indexBlockSize = (128 * 4 + 1 * 8) << 17;
    private final int indexBlockSize2 = 128 << 17;
    private final int dataBlockSize = 128 << 20;
    private MappedByteBuffer indexBuffer, dataBuffer;


    public IndexedChronicle(String basePath) throws FileNotFoundException {
        this.indexFile = new RandomAccessFile(basePath + ".index", "rw").getChannel();
        this.dataFile = new RandomAccessFile(basePath + ".data", "rw").getChannel();
    }

    @Override
    public void close() throws IOException {
        this.indexFile.close();
        this.dataFile.close();
    }

    @Override
    public Excerpt createExcerpt() {
        return new ChronicleExcerpt();
    }

    private long acquireIndexAddress(long index) {
        try {
            long section = index / indexBlockSize2;
            indexBuffer = indexFile.map(FileChannel.MapMode.READ_WRITE, section * indexBlockSize, indexBlockSize);
            return ((DirectBuffer) indexBuffer).address();

        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private long acquireDataAddress(long dataIndex) {
        try {
            long section = dataIndex / dataBlockSize;
            dataBuffer = dataFile.map(FileChannel.MapMode.READ_WRITE, section * dataBlockSize, dataBlockSize);
            return ((DirectBuffer) dataBuffer).address();

        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    class ChronicleExcerpt extends NativeBytes implements Excerpt {
        long index = -1;
        long indexAddress = 0;
        long indexLimit = 0;
        long dataStartAddress = 0;
        long dataStartOffset = dataBlockSize;
        long dataAddressRealBase = 0;
        int dataAddressOffset = 0;
        int dataAddressOffsetFromIndex = 0;

        ChronicleExcerpt() {
            super(0, 0, 0);
        }

        @Override
        public void startExcerpt(int capacity) {
            index++;
            if (indexAddress >= indexLimit)
                acquireNextIndex();
            if (capacity < 8) capacity = 8;
            if (dataAddressOffset + capacity >= dataBlockSize)
                acquireNextData();
            start = dataStartAddress + dataStartOffset;
            position = start;
            limit = start + capacity;
            dataAddressOffsetFromIndex += capacity;
        }

        private void acquireNextData() {
//            acquireDataAddress()
        }

        private void acquireNextIndex() {
            indexAddress = acquireIndexAddress(index);
            indexLimit = indexAddress + indexBlockSize;
            UNSAFE.putLong(indexAddress, dataAddressRealBase + dataAddressOffset);
            indexAddress += 8;
            dataAddressOffsetFromIndex = 0;
        }

        @Override
        public void finish() {
            UNSAFE.putOrderedInt(null, indexAddress, dataAddressOffsetFromIndex);
            indexAddress += 4;
        }


    }
}
