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
package net.openhft.chronicle;


import net.openhft.lang.Jvm;
import net.openhft.lang.model.constraints.NotNull;

import java.io.File;
import java.io.IOException;

public abstract class ChronicleQueueBuilder implements Cloneable {

    private final File path;
    private final Class<? extends Chronicle> type;

    private boolean synchronous;
    private boolean useCheckedExcerpt;

    /**
     * @param type
     * @param path
     */
    private ChronicleQueueBuilder(Class<? extends Chronicle> type, File path) {
        this.type = type;
        this.path = path;
        this.synchronous = false;
        this.useCheckedExcerpt = false;
    }

    protected Class<? extends Chronicle> type() {
        return this.type;
    }

    protected File path() {
        return this.path;
    }

    /**
     * Sets the synchronous mode to be used. Enabling synchronous mode means that
     * {@link ExcerptCommon#finish()} will force a persistence every time.
     *
     * @param synchronous If synchronous mode should be used or not.
     *
     * @return this builder object back
     */
    public ChronicleQueueBuilder synchronous(boolean synchronous) {
        this.synchronous = synchronous;
        return this;
    }

    /**
     * Checks if synchronous mode is enabled or not.
     *
     * @return true if synchronous mode is enabled, false otherwise.
     */
    public boolean synchronous() {
        return this.synchronous;
    }

    /**
     *
     * @param useCheckedExcerpt
     *
     * @return this builder object back
     */
    public ChronicleQueueBuilder useCheckedExcerpt(boolean useCheckedExcerpt) {
        this.useCheckedExcerpt = useCheckedExcerpt;
        return this;
    }

    public boolean useCheckedExcerpt() {
        return this.useCheckedExcerpt;
    }

    public abstract Chronicle build() throws IOException;

    // *************************************************************************
    //
    // *************************************************************************

    public static IndexedChronicleQueueBuilder indexed(File path) {
        return new IndexedChronicleQueueBuilder(path);
    }

    public static IndexedChronicleQueueBuilder indexed(String path) {
        return indexed(new File(path));
    }

    public static IndexedChronicleQueueBuilder indexed(String parent, String child) {
        return indexed(new File(parent, child));
    }

    public static IndexedChronicleQueueBuilder indexed(File parent, String child) {
        return indexed(new File(parent, child));
    }


    public static VanillaChronicleQueueBuilder vanilla(File path) {
        return new VanillaChronicleQueueBuilder(path);
    }

    public static ChronicleQueueBuilder vanilla(String path) {
        return vanilla(new File(path));
    }

    public static ChronicleQueueBuilder vanilla(String parent, String child) {
        return vanilla(new File(parent, child));
    }

    public static ChronicleQueueBuilder vanilla(File parent, String child) {
        return vanilla(new File(parent, child));
    }

    // *************************************************************************
    //
    // *************************************************************************

    public static class IndexedChronicleQueueBuilder extends ChronicleQueueBuilder implements Cloneable {

        private int cacheLineSize;
        private int dataBlockSize;
        private int messageCapacity;
        private int indexBlockSize;

        private IndexedChronicleQueueBuilder(final File path) {
            super(IndexedChronicle.class, path);

            this.cacheLineSize   = 64;
            this.dataBlockSize   = Jvm.is64Bit() ? 128 * 1024 * 1024 : 16 * 1024 * 1024;
            this.indexBlockSize  = Math.max(4096, this.dataBlockSize / 4);
            this.messageCapacity = 128 * 1024;
        }

        /**
         * Sets the size of the index cache lines. Index caches (files) consist
         * of fixed size lines, each line having ultiple index entries on it. This
         * param specifies the size of such a multi entry line.
         *
         * Default value is <b>64</b>.
         *
         * @param cacheLineSize the size of the cache lines making up index files
         *
         * @return this builder object back
         */
        public IndexedChronicleQueueBuilder cacheLineSize(int cacheLineSize) {
            this.cacheLineSize = cacheLineSize;
            return this;
        }

        /**
         * The size of the index cache lines (index caches are made up of multiple
         * fixed length lines, each line contains multiple index entries).
         *
         * Default value is <b>64</b>.
         *
         * @return the size of the index cache lines
         */
        public int cacheLineSize() {
            return this.cacheLineSize;
        }

        /**
         * Sets the size to be used for data blocks. The method also has a side
         * effect. If the data block size specified is smaller than twice the
         * message capacity, then the message capacity will be adjusted to equal
         * half of the new data block size.
         *
         * @param dataBlockSize the size of the data blocks
         *
         * @return this builder object back
         */
        public IndexedChronicleQueueBuilder dataBlockSize(int dataBlockSize) {
            this.dataBlockSize = dataBlockSize;
            if (messageCapacity > dataBlockSize / 2) {
                messageCapacity = dataBlockSize / 2;
            }

            return this;
        }

        /**
         * Returns the size of the data blocks.
         *
         * @return the size of the data blocks
         */
        public int dataBlockSize() {
            return this.dataBlockSize;
        }

        /**
         * Sets the size to be used for index blocks. Is capped to the bigger
         * value among <b>4096</b> and a <b>quarter of the <tt>data block size</tt></b>.
         *
         * @param indexBlockSize the size of the index blocks
         *
         * @return this builder object back
         */
        public IndexedChronicleQueueBuilder indexBlockSize(int indexBlockSize) {
            this.indexBlockSize = indexBlockSize;
            return this;
        }

        /**
         * Returns the size of the index blocks.
         *
         * @return the size of the index blocks
         */
        public int indexBlockSize() {
            return this.indexBlockSize;
        }

        /**
         * The maximum size a message stored in a {@link net.openhft.chronicle.Chronicle}
         * instance can have. Defaults to <b>128K</b>. Is limited by the <tt>data block size</tt>,
         * can't be bigger than half of the data block size.
         *
         * @param messageCapacity the maximum message size that can be stored
         *
         * @return tthis builder object back
         */
        public IndexedChronicleQueueBuilder messageCapacity(int messageCapacity) {
            this.messageCapacity = messageCapacity;
            return this;
        }

        /**
         * The maximum size of the message that can be stored in the {@link net.openhft.chronicle.Chronicle}
         * instance to be configured. Defaults to <b>128K</b>. Can't be bigger
         * than half of the <tt>data block size</tt>.
         *
         * @return the maximum message size that can be stored
         */
        public int messageCapacity() {
            return this.messageCapacity;
        }

        /**
         * A pre-defined ChronicleBuilder for small {@link net.openhft.chronicle.Chronicle} instances.
         *
         * It has the following params:
         * <ul>
         * <li>data block size <b>16M</b></li>
         * </ul>
         */
        public IndexedChronicleQueueBuilder small() {
            dataBlockSize(16 * 1024 * 1024);
            return this;
        }

        /**
         * A pre-defined ChronicleBuilder for medium {@link net.openhft.chronicle.Chronicle} instances.
         *
         * It has the following params:
         * <ul>
         * <li>data block size <b>128M</b></li>
         * </ul>
         */
        public IndexedChronicleQueueBuilder medium() {
            dataBlockSize(128 * 1024 * 1024);
            return this;
        }

        /**
         * A pre-defined ChronicleBuilder for large {@link net.openhft.chronicle.Chronicle} instances.
         *
         * It has the following params:
         * <ul>
         * <li>data block size <b>512M</b></li>
         * </ul>
         */
        public IndexedChronicleQueueBuilder large() {
            dataBlockSize(512 * 1024 * 1024);
            return this;
        }

        /**
         * A pre-defined ChronicleBuilder for test {@link net.openhft.chronicle.Chronicle} instances.
         *
         * It has the following params:
         * <ul>
         * <li>data block size <b>8k</b></li>
         * </ul>
         */
        public IndexedChronicleQueueBuilder test() {
            dataBlockSize(8 * 1024);
            return this;
        }

        /**
         * The default ChronicleConfig used by various {@link net.openhft.chronicle.Chronicle}
         * implementations if not otherwise specified.
         *
         * On 64 bit JVMs it delegates to {@link net.openhft.chronicle.ChronicleQueueBuilder.IndexedChronicleQueueBuilder#medium()},
         * On 32 bit JVMs it delegates to {@link net.openhft.chronicle.ChronicleQueueBuilder.IndexedChronicleQueueBuilder#small()},
         */
        public IndexedChronicleQueueBuilder standard() {
            return Jvm.is64Bit() ? medium() : small();
        }

        @Override
        public Chronicle build() throws IOException {
            return new IndexedChronicle(this);
        }

        /**
         * Makes IndexedChronicleQueueBuilder cloneable.
         *
         * @return a cloned copy of this IndexedChronicleQueueBuilder instance
         */
        @NotNull
        @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
        @Override
        public IndexedChronicleQueueBuilder clone() {
            try {
                return (IndexedChronicleQueueBuilder) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new AssertionError(e);
            }
        }
    }

    private static class VanillaChronicleQueueBuilder extends ChronicleQueueBuilder {
        private String cycleFormat = "yyyyMMdd";
        private int cycleLength = 24 * 60 * 60 * 1000; // MILLIS_PER_DAY
        private int defaultMessageSize = 128 << 10; // 128 KB.
        private int dataCacheCapacity = 32;
        private int indexCacheCapacity = 32;
        private long indexBlockSize = 16L << 20; // 16 MB
        private long dataBlockSize = 64L << 20; // 64 MB
        private long entriesPerCycle = 1L << 40; // one trillion per day or per hour.
        private boolean cleanupOnClose = false;

        private VanillaChronicleQueueBuilder(File path) {
            super(VanillaChronicle.class, path);
        }

        @Override
        public Chronicle build() throws IOException {
            return null;
        }
    }

    /*
    private class ReplicaConfig {
        protected InetSocketAddress bindAddress = null;
        protected InetSocketAddress connectAddress = null;
        protected long reconnectTimeout = 500;
        protected TimeUnit reconnectTimeoutUnit = TimeUnit.MILLISECONDS;
        protected long selectTimeout = 1000;
        protected TimeUnit selectTimeoutUnit = TimeUnit.MILLISECONDS;
        protected int maxOpenAttempts = Integer.MAX_VALUE;
        protected int receiveBufferSize = 256 * 1024;
    }
    */

    // *************************************************************************
    //
    // *************************************************************************

    /**
     * Makes ChronicleQueueBuilder cloneable.
     *
     * @return a cloned copy of this ChronicleConfig instance
     */
    @Override
    public ChronicleQueueBuilder clone() throws CloneNotSupportedException {
        return (ChronicleQueueBuilder) super.clone();
    }
}
