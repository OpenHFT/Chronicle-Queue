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

import net.openhft.chronicle.*;
import net.openhft.chronicle.tools.WrappedExcerpt;
import net.openhft.lang.model.constraints.NotNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * User: peter.lawrey
 * Date: 26/09/13
 * Time: 17:19
 */
public class RollingChronicle implements Chronicle {

    @NotNull
    private final String basePath;
    @NotNull
    private final ChronicleConfig config;
    @NotNull
    private final SingleMappedFileCache masterFileCache;
    @NotNull
    private final ByteBuffer masterMBB;
    private int nextIndex;
    private IndexedChronicleCache chronicleCache;
    private long lastWriitenIndex;

    public RollingChronicle(@NotNull String basePath, @NotNull ChronicleConfig config) throws FileNotFoundException {
        this.basePath = basePath;
        this.config = config;
        nextIndex = 0;
        new File(basePath).mkdirs();

        masterFileCache = new SingleMappedFileCache(basePath + "/master", config.indexFileCapacity() * 4);
        masterMBB = masterFileCache.acquireBuffer(0, false).order(ByteOrder.nativeOrder());
        findLastIndex();
        rollNewIndexFileData();

    }

    private void findLastIndex() {
        int indexFileExcerpts = config().indexFileExcerpts();
        for (int i = 0; i < masterMBB.capacity() - 3; i += 4) {
            int used = masterMBB.getInt(i);
            if (used < indexFileExcerpts) {
                nextIndex = i / 4;
                lastWriitenIndex = (i / 4) * indexFileExcerpts + used - 1;
                return;
            }
        }
        throw new IllegalStateException("The master file has been exhausted.");
    }

    private void rollNewIndexFileData() {
        chronicleCache = new IndexedChronicleCache(basePath);
    }

    @Override
    public String name() {
        return basePath;
    }

    @NotNull
    @Override
    public Excerpt createExcerpt() throws IOException {
        return new RollingExcerpt(Type.Excerpt);
    }

    @NotNull
    @Override
    public ExcerptTailer createTailer() throws IOException {
        return new RollingExcerpt(Type.Tailer);
    }

    @NotNull
    @Override
    public ExcerptAppender createAppender() throws IOException {
        return new RollingExcerpt(Type.Appender);
    }

    @Override
    public long lastIndex() {
        return lastWrittenIndex();
    }

    @Override
    public long lastWrittenIndex() {
        return lastWriitenIndex;
    }

    @Override
    public long size() {
        return lastWrittenIndex() + 1;
    }

    ChronicleConfig config() {
        return config;
    }

    @Override
    public void close() throws IOException {
        chronicleCache.close();
        masterFileCache.close();
    }

    @Override
    public void clear() {
    }

    enum Type {
        Appender, Tailer, Excerpt
    }

    class RollingExcerpt extends WrappedExcerpt {
        private final Type type;
        private final int indexFileExcerpts = config().indexFileExcerpts();
        private long chronicleIndexBase = Long.MIN_VALUE;
        private IndexedChronicle chronicle;

        public RollingExcerpt(Type type) {
            super(null);
            this.type = type;
        }

        @Override
        public long size() {
            return RollingChronicle.this.size();
        }

        @Override
        public boolean nextIndex() {
            return checkNextChronicle(1);
        }

        @Override
        public Chronicle chronicle() {
            return RollingChronicle.this;
        }

        @Override
        public boolean index(long index) throws IndexOutOfBoundsException {
            try {
                int chronicleIndex0 = (int) (index / indexFileExcerpts);
                int chronicleIndex1 = (int) (index % indexFileExcerpts);
                long newBase = (long) chronicleIndex0 * indexFileExcerpts;
                if (newBase != chronicleIndexBase) {
                    chronicleIndexBase = newBase;
                    chronicle = chronicleCache.acquireChronicle(chronicleIndex0);
                    nextIndex = chronicleIndex0;
                    switch (type) {
                        case Tailer:
                            setExcerpt(chronicle.createTailer());
                            break;
                        case Excerpt:
                            setExcerpt(chronicle.createExcerpt());
                            break;
                        case Appender:
                            setExcerpt(chronicle.createAppender());
                            return true;
                    }
                }
                return (type == Type.Appender) || super.index(chronicleIndex1);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }

        @Override
        public void startExcerpt() {
            startExcerpt(config().messageCapacity());
        }

        @Override
        public void startExcerpt(long capacity) {
            index(RollingChronicle.this.size());
            super.startExcerpt(capacity);
        }

        private boolean checkNextChronicle(int n) {
            if (chronicle == null) {
                return index(0);

            } else if (super.index() + n >= indexFileExcerpts) {
                boolean ret = index(index() + n);
                nextIndex = (int) (chronicleIndexBase / indexFileExcerpts);
                return ret;
            } else if (n > 0) {
                return super.nextIndex();
            }
            return true;
        }

        @Override
        public long index() {
            return chronicleIndexBase + super.index();
        }

        @Override
        public void finish() {
            super.finish();
            masterMBB.putInt(nextIndex << 2, (int) chronicle.size());
            lastWriitenIndex++;
        }
    }
}
