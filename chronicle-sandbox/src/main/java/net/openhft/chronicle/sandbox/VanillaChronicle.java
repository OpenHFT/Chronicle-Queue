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

import net.openhft.affinity.AffinitySupport;
import net.openhft.chronicle.*;
import net.openhft.lang.Maths;
import net.openhft.lang.io.DirectBytes;
import net.openhft.lang.io.DirectStore;
import net.openhft.lang.io.NativeBytes;
import net.openhft.lang.io.serialization.BytesMarshallerFactory;
import net.openhft.lang.io.serialization.impl.VanillaBytesMarshallerFactory;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.lang.ref.WeakReference;

/**
 * Created by peter
 */
public class VanillaChronicle implements Chronicle {
    private final String name;
    private final String basePath;
    private final VanillaChronicleConfig config;
    private final ThreadLocal<WeakReference<BytesMarshallerFactory>> marshallersCache = new ThreadLocal<WeakReference<BytesMarshallerFactory>>();
    private final ThreadLocal<WeakReference<ExcerptTailer>> tailerCache = new ThreadLocal<WeakReference<ExcerptTailer>>();
    private final ThreadLocal<WeakReference<ExcerptAppender>> appenderCache = new ThreadLocal<WeakReference<ExcerptAppender>>();
    private final DirectBytes NO_BYTES = DirectStore.allocateLazy(4096).createSlice();
    private final VanillaIndexCache indexCache;
    private final VanillaDataCache dataCache;
    //    private volatile int cycle;
    private volatile long lastWrittenIndex;

    public VanillaChronicle(String basePath) {
        this(basePath, VanillaChronicleConfig.DEFAULT);
    }

    public VanillaChronicle(String basePath, VanillaChronicleConfig config) {
        this.basePath = basePath;
        this.config = config;
        name = new File(basePath).getName();
        DateCache dateCache = new DateCache(config.cycleFormat(), config.cycleLength());
        indexCache = new VanillaIndexCache(basePath, Maths.intLog2(config.indexBlockSize()), dateCache);
        dataCache = new VanillaDataCache(basePath, Maths.intLog2(config.dataBlockSize()), dateCache);
//        cycle = (int) (System.currentTimeMillis() / config.cycleLength());
    }

    @Override
    public String name() {
        return name;
    }

    @NotNull
    @Override
    public Excerpt createExcerpt() throws IOException {
        throw new UnsupportedOperationException();
    }

    protected BytesMarshallerFactory acquireBMF() {
        WeakReference<BytesMarshallerFactory> bmfRef = marshallersCache.get();
        BytesMarshallerFactory bmf = null;
        if (bmfRef != null)
            bmf = bmfRef.get();
        if (bmf == null) {
            bmf = createBMF();
            marshallersCache.set(new WeakReference<BytesMarshallerFactory>(bmf));
        }
        return bmf;
    }

    protected BytesMarshallerFactory createBMF() {
        return new VanillaBytesMarshallerFactory();
    }

    @NotNull
    @Override
    public ExcerptTailer createTailer() throws IOException {
        WeakReference<ExcerptTailer> ref = tailerCache.get();
        ExcerptTailer tailer = null;
        if (ref != null)
            tailer = ref.get();
        if (tailer == null) {
            tailer = createTailer0();
            tailerCache.set(new WeakReference<ExcerptTailer>(tailer));
        }
        return tailer;
    }

    private ExcerptTailer createTailer0() {
        return new VanillaTailer();
    }

    @NotNull
    @Override
    public ExcerptAppender createAppender() throws IOException {
        WeakReference<ExcerptAppender> ref = appenderCache.get();
        ExcerptAppender appender = null;
        if (ref != null)
            appender = ref.get();
        if (appender == null) {
            appender = createAppender0();
            appenderCache.set(new WeakReference<ExcerptAppender>(appender));
        }
        return appender;
    }

    private ExcerptAppender createAppender0() {
        return new VanillaAppender();
    }

    @Override
    public long lastWrittenIndex() {
        return lastWrittenIndex;
    }

    @Override
    public long size() {
        return lastWrittenIndex + 1;
    }

    @Override
    public void close() throws IOException {

    }

    abstract class AbstractVanillaExcerpt extends NativeBytes implements ExcerptCommon {
        public AbstractVanillaExcerpt() {
            super(acquireBMF(), NO_BYTES.startAddr(), NO_BYTES.startAddr(), NO_BYTES.startAddr());
        }

        @Override
        public boolean wasPadding() {
            return false;
        }

        @Override
        public long index() {
            return 0;
        }

        @Override
        public long lastWrittenIndex() {
            return VanillaChronicle.this.lastWrittenIndex();
        }

        @Override
        public long size() {
            return lastWrittenIndex() + 1;
        }

        @Override
        public ExcerptCommon toEnd() {
            return null;
        }

        @Override
        public Chronicle chronicle() {
            return VanillaChronicle.this;
        }

        public int cycle() {
            return (int) (System.currentTimeMillis() / config.cycleLength());
        }
    }

    class VanillaExcerpt extends AbstractVanillaExcerpt implements Excerpt {
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

        @Override
        public boolean nextIndex() {
            throw new UnsupportedOperationException();
        }

        @NotNull
        @Override
        public Excerpt toStart() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Excerpt toEnd() {
            super.toEnd();
            return this;
        }

        @Override
        public void finish() {
            super.finish();
        }
    }

    class VanillaAppender extends AbstractVanillaExcerpt implements ExcerptAppender {
        private int appenderCycle, appenderThreadId;
        private boolean nextSynchronous;
        private VanillaFile appenderFile;
//        int indexFile;
//        int lastCycle;
//        int idInIndex;

        VanillaAppender() {
//            indexFile = indexCache.lastIndexFile(cycle);
        }

        @Override
        public void startExcerpt() {
            startExcerpt(config.defaultMessageSize());
        }

        @Override
        public void startExcerpt(long capacity) {
            try {
                appenderCycle = cycle();
                appenderThreadId = AffinitySupport.getThreadId();
                appenderFile = dataCache.dataForLast(appenderCycle, appenderThreadId);
                if (appenderFile.bytes().remaining() < capacity + 4) {
                    dataCache.incrementLastCount();
                    appenderFile = dataCache.dataForLast(appenderCycle, appenderThreadId);
                }
                startAddr = positionAddr = appenderFile.bytes().positionAddr() + 4;
                limitAddr = startAddr + capacity;
                nextSynchronous = config.synchronous();
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }

        @Override
        public void addPaddedEntry() {
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
            int length = (int) (positionAddr - startAddr);
            NativeBytes.UNSAFE.putOrderedInt(null, startAddr - 4, length);
            // position of the start not the end.
            int offset = (int) (startAddr - appenderFile.baseAddr());
            try {
                indexCache.append(appenderCycle, appenderThreadId, appenderFile.indexCount() * config.dataBlockSize() + offset);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
            appenderFile.bytes().positionAddr((positionAddr + 3) & ~3L);
        }

        @Override
        public ExcerptAppender toEnd() {
            super.toEnd();
            return this;
        }
    }

    class VanillaTailer extends AbstractVanillaExcerpt implements ExcerptTailer {


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
            super.toEnd();
            return this;
        }

        @Override
        public void finish() {
            super.finish();
        }
    }
}
