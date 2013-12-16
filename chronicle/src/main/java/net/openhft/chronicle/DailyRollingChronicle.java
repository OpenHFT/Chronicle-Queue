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

import net.openhft.chronicle.tools.MasterIndexFile;
import net.openhft.lang.Maths;
import net.openhft.lang.io.DirectBytes;
import net.openhft.lang.io.DirectStore;
import net.openhft.lang.io.NativeBytes;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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
    private final SimpleDateFormat dateFormat;
    private final String currentFilename;
    private final List<DRFiles> filesList;
    private volatile DRFiles indexFiles;
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
        dateFormat = new SimpleDateFormat(config.getFileFormat());
        dateFormat.setTimeZone(config.getTimeZone());
        currentFilename = dateFormat.format(new Date());
        int index = master.append(currentFilename);
        filesList = new ArrayList<DRFiles>(index + 128);
        while (filesList.size() < index)
            filesList.add(null);
        indexFiles = new DRFiles(file, currentFilename);
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
        throw new UnsupportedOperationException();
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

        private void findTheLastIndex() throws IOException {
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
            throw new UnsupportedOperationException();
        }

        @Override
        public long size() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExcerptCommon toEnd() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Chronicle chronicle() {
            throw new UnsupportedOperationException();
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

        @Override
        public boolean index(long l) {
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

        @Override
        public Excerpt toEnd() {
            return this;
        }
    }

    class DRCExcerptTailer extends AbstractDRCExcerpt implements ExcerptTailer {
        @Override
        public boolean index(long l) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean nextIndex() {
            throw new UnsupportedOperationException();
        }

        @NotNull
        @Override
        public ExcerptTailer toStart() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExcerptTailer toEnd() {
            return this;
        }
    }

    class DRCExcerptAppender extends AbstractDRCExcerpt implements ExcerptAppender {
        private boolean nextSynchronous = config.isSynchronousWriter();

        @Override
        public void startExcerpt() {
            startExcerpt(config.getMaxEntrySize());
        }

        @Override
        public void startExcerpt(long capacity) {
            // TODO !!!
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
            nextSynchronous = config.isSynchronousWriter();
        }

        @Override
        public ExcerptAppender toEnd() {
            return this;
        }
    }
}
