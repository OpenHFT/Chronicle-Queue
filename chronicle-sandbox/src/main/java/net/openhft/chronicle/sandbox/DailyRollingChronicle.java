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

package net.openhft.chronicle.sandbox;

import net.openhft.chronicle.*;
import net.openhft.chronicle.tools.MasterIndexFile;
import net.openhft.lang.Maths;
import net.openhft.lang.io.DirectBytes;
import net.openhft.lang.io.DirectStore;
import net.openhft.lang.io.NativeBytes;
import net.openhft.lang.model.constraints.NotNull;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author peter.lawrey
 */
public class DailyRollingChronicle implements Chronicle {
    private static final int INDEX_SAMPLE = 8; // only index every 8th entry.
    private static final int INDEX_SAMPLE_BITS = Maths.intLog2(INDEX_SAMPLE);
    //    private final ScheduledExecutorService worker;
    private final File file;
    private final MasterIndexFile master;
    private final DailyRollingConfig config;
    private final List<DRFiles> filesList;
    private DirectBytes bytes;
    private DRCExcerptAppender appender;
    private long lastWrittenIndex = -1;

    public DailyRollingChronicle(String filename, DailyRollingConfig config) throws IOException {
        this.config = config;
        this.file = new File(filename);
        file.mkdirs();
        if (!file.isDirectory())
            throw new IOException("Failed to create directory " + file);
        master = new MasterIndexFile(new File(file, "master"));
        bytes = new DirectStore(config.getBytesMarshallerFactory(), config.getMaxEntrySize(), false).createSlice();
        SimpleDateFormat dateFormat = new SimpleDateFormat(config.getFileFormat());
        dateFormat.setTimeZone(config.getTimeZone());
        String currentFilename = dateFormat.format(new Date());
        int index = master.append(currentFilename);
        filesList = new CopyOnWriteArrayList<DRFiles>();
        while (filesList.size() < index)
            filesList.add(null);
        DRFiles indexFiles = new DRFiles(file, currentFilename);
        filesList.add(indexFiles);
//        worker = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory(file.getName() + "-worker", true));
    }

    @Override
    public String name() {
        return file.getName();
    }

    @NotNull
    @Override
    public Excerpt createExcerpt() throws IOException {
        return new DRCExcerpt();
    }

    @NotNull
    @Override
    public ExcerptTailer createTailer() throws IOException {
        return new DRCExcerptTailer();
    }

    @NotNull
    @Override
    public ExcerptAppender createAppender() throws IOException {
        if (appender == null)
            appender = new DRCExcerptAppender();
        return appender;
    }

    @Override
    public long lastWrittenIndex() {
        if (lastWrittenIndex < 0)
            findLastIndex();
        return lastWrittenIndex;
    }

    private void findLastIndex() {
        int index = filesList.size() - 1;
        lastWrittenIndex = index * config.getMaxEntriesPerCycle() + filesList.get(index).findTheLastIndex();
    }

    @Override
    public long size() {
        return lastWrittenIndex + 1;
    }

    public DailyRollingConfig config() {
        return config;
    }

    @Override
    public void close() throws IOException {
        master.close();
        for (DRFiles drFiles : filesList) {
            drFiles.close();
        }
    }

    synchronized DRFiles acquireDRFile(int fileIndex) {
        DRFiles drFiles = filesList.get(fileIndex);
        if (drFiles == null) {
            String filename = master.filenameFor(fileIndex);
            try {
                filesList.set(fileIndex, drFiles = new DRFiles(file, filename));
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
        return drFiles;
    }

    class DRFiles {
        private final MappedFileCache indexMappedCache, dataMappedCache;
        private final List<MappedByteBuffer> indexBuffers = new ArrayList<MappedByteBuffer>();
        private final int indexBlockSize, dataBlockSize;

        public DRFiles(File dir, String filename) throws IOException {
            indexBlockSize = config.getIndexBlockSize();
            dataBlockSize = config.getDataBlockSize();
            this.indexMappedCache = new SingleMappedFileCache(new File(dir, filename + ".index"), indexBlockSize);
            this.dataMappedCache = new SingleMappedFileCache(new File(dir, filename + ".data"), dataBlockSize);
            findTheLastIndex();
        }

        private long findTheLastIndex() {
            long lastBlock = (indexMappedCache.size() - 8) / indexBlockSize;
            MappedByteBuffer imbb = null;
            for (; lastBlock >= 0; lastBlock--) {
                imbb = indexMappedCache.acquireBuffer(lastBlock, false);
                if (imbb.getLong(0) > 0)
                    break;
            }
            long size = 0;
            long dataOffset = 0;
            if (lastBlock >= 0 && imbb != null) {
                int lo = -1, hi = indexBlockSize >>> 3;
                while (lo + 1 < hi) {
                    int mid = (hi + lo) >>> 1;
                    dataOffset = imbb.getLong(mid << 3);
                    if (dataOffset > 0)
                        lo = mid;
                    else
                        hi = mid;
                }
                size = (lastBlock * indexBlockSize >>> 3) + lo;
            }
            // follow the offsets to find the entries.
            long dataBlockIndex = dataOffset / dataBlockSize;
            MappedByteBuffer dmbb = dataMappedCache.acquireBuffer(dataBlockIndex, false);
            int offset = (int) (dataOffset % dataBlockSize);
            while (true) {
                int excerptSize = dmbb.getInt(offset);
                if (excerptSize == 0) {
                    lastWrittenIndex = size - 1;
                    break;
                }
                if (excerptSize > config.getMaxEntrySize())
                    throw new AssertionError("Over sized entry of " + (dataBlockIndex * dataBlockSize + offset));
                if (excerptSize < 0)
                    throw new AssertionError("Negative size !! at " + (dataBlockIndex * dataBlockSize + offset));
                // offsets are placed 4-byte word aligned.
                offset += (excerptSize + 3) & ~3;
                size++;
                if ((size & (INDEX_SAMPLE - 1)) == 0)
                    addIndexEntry(size >>> INDEX_SAMPLE_BITS, dataBlockIndex * dataBlockSize + offset);
                if (offset > dataBlockSize) {
                    dataBlockIndex++;
                    dmbb = dataMappedCache.acquireBuffer(dataBlockIndex, false);
                    offset -= dataBlockSize;
                }
            }
            return lastWrittenIndex;
        }

        private void addIndexEntry(long indexPosition, long dataPositon) {
            // TODO
        }

        public void close() {
            indexMappedCache.close();
            dataMappedCache.close();
        }
    }

    class AbstractDRCExcerpt extends NativeBytes implements ExcerptCommon {
        long index = -1;
        private MappedByteBuffer dataMBB;

        public AbstractDRCExcerpt() {
            super(bytes);
        }

        @Override
        public boolean wasPadding() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long index() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long lastWrittenIndex() {
            return DailyRollingChronicle.this.lastWrittenIndex();
        }

        @Override
        public long size() {
            return capacity();
        }

        @NotNull
        @Override
        public ExcerptCommon toEnd() {
            index(lastWrittenIndex());
            return this;
        }

        @Override
        public Chronicle chronicle() {
            throw new UnsupportedOperationException();
        }

        public boolean index(long l) {
            if (l < 0)
                return false;
            long fileIndex = l / config.getMaxEntriesPerCycle();
            if (fileIndex >= filesList.size())
                return false;
            DRFiles drFiles = filesList.get((int) fileIndex);
            if (drFiles == null)
                drFiles = acquireDRFile((int) fileIndex);
            long index2 = l % config.getMaxEntriesPerCycle() >> INDEX_SAMPLE_BITS << 3;
            MappedByteBuffer indexMBB = drFiles.indexMappedCache.acquireBuffer(index2, false);
            long dataOffset = indexMBB.getLong((int) index2);
            int dataBlockSize = config.getDataBlockSize();
            for (int indexOffset = (int) (l & (INDEX_SAMPLE - 1)); indexOffset > 0; indexOffset--) {
                MappedByteBuffer dataMBB = drFiles.dataMappedCache.acquireBuffer(dataOffset / dataBlockSize, false);
                int size = dataMBB.getInt((int) (dataOffset % dataBlockSize));
                dataOffset += (size + 3) & ~3; // 4 - byte alignment.
            }
            dataMBB = drFiles.dataMappedCache.acquireBuffer(dataOffset / dataBlockSize, false);
            long offsetInDataBlock = dataOffset % dataBlockSize + 4;
            startAddr = positionAddr = ((DirectBuffer) dataMBB).address() + offsetInDataBlock;
            int size = NativeBytes.UNSAFE.getInt(startAddr - 4);
            assert offsetInDataBlock + size <= dataBlockSize;
            limitAddr = startAddr + size;
            return true;
        }
    }

    class DRCExcerpt extends AbstractDRCExcerpt implements Excerpt {
        @Override
        public long findMatch(@NotNull ExcerptComparator comparator) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void findRange(@NotNull long[] startEnd, @NotNull ExcerptComparator comparator) {
            throw new UnsupportedOperationException();
        }

        @NotNull
        @Override
        public Excerpt toStart() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean nextIndex() {
            throw new UnsupportedOperationException();
        }

        @NotNull
        @Override
        public Excerpt toEnd() {
            super.toEnd();
            return this;
        }
    }

    class DRCExcerptTailer extends AbstractDRCExcerpt implements ExcerptTailer {
        @Override
        public boolean nextIndex() {
            startAddr = (limitAddr + 3) & ~3;
            int size = NativeBytes.UNSAFE.getInt(startAddr);
            startAddr = positionAddr = startAddr + 4;
            limitAddr = startAddr + size;
            index++;
            return true;
        }

        @NotNull
        @Override
        public ExcerptTailer toStart() {
            index = -1;
            return this;
        }

        @NotNull
        @Override
        public ExcerptTailer toEnd() {
            super.toEnd();
            return this;
        }
    }

    class DRCExcerptAppender extends AbstractDRCExcerpt implements ExcerptAppender {
        private boolean nextSynchronous = config.isSynchronousWriter();

        DRCExcerptAppender() {
            toEnd();
        }

        @Override
        public void startExcerpt() {
            startExcerpt(config.getMaxEntrySize());
        }

        @Override
        public void startExcerpt(long capacity) {
            startAddr = positionAddr = ((limitAddr + 3) & ~3) + 4;
            limitAddr = startAddr + capacity;
        }

        @Override
        public void addPaddedEntry() {
            // Not implemented.
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean nextSynchronous() {
            return nextSynchronous;
        }

        @Override
        public void nextSynchronous(boolean nextSynchronous) {
            this.nextSynchronous = nextSynchronous;
        }

        @Override
        public void finish() {
            super.finish();
            long size = positionAddr - startAddr;
            NativeBytes.UNSAFE.putOrderedInt(null, startAddr - 4, (int) size);
            nextSynchronous = config.isSynchronousWriter();
        }

        @NotNull
        @Override
        public ExcerptAppender toEnd() {
            super.toEnd();
            return this;
        }
    }
}
