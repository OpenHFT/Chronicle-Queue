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

import net.openhft.chronicle.tcp.*;
import net.openhft.lang.Jvm;
import net.openhft.lang.model.constraints.NotNull;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public abstract class ChronicleQueueBuilder implements Cloneable {

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

    public static VanillaChronicleQueueBuilder vanilla(String path) {
        return vanilla(new File(path));
    }

    public static VanillaChronicleQueueBuilder vanilla(String parent, String child) {
        return vanilla(new File(parent, child));
    }

    public static VanillaChronicleQueueBuilder vanilla(File parent, String child) {
        return vanilla(new File(parent, child));
    }

    public static ReplicaChronicleQueueBuilder statelessSink() {
        return sink(null);
    }

    public static ReplicaChronicleQueueBuilder sink(Chronicle chronicle) {
        return new SinkChronicleQueueBuilder(chronicle);
    }

    public static ReplicaChronicleQueueBuilder source(Chronicle chronicle) {
        return new SourceChronicleQueueBuilder(chronicle);
    }

    // *************************************************************************
    //
    // *************************************************************************

    public static class IndexedChronicleQueueBuilder extends ChronicleQueueBuilder implements Cloneable {

        private final File path;

        private boolean synchronous;
        private boolean useCheckedExcerpt;

        private int cacheLineSize;
        private int dataBlockSize;
        private int messageCapacity;
        private int indexBlockSize;

        /**
         * On 64 bit JVMs it has the following params:
         * <ul>
         * <li>data block size <b>128M</b></li>
         * </ul>
         *
         * On 32 bit JVMs it has the following params:
         * <ul>
         * <li>data block size <b>16M</b></li>
         * </ul>
         */
        private IndexedChronicleQueueBuilder(final File path) {
            this.path  = path;
            this.synchronous = false;
            this.useCheckedExcerpt = false;
            this.cacheLineSize = 64;
            this.dataBlockSize = Jvm.is64Bit() ? 128 * 1024 * 1024 : 16 * 1024 * 1024;
            this.indexBlockSize = Math.max(4096, this.dataBlockSize / 4);
            this.messageCapacity = 128 * 1024;
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
        public IndexedChronicleQueueBuilder synchronous(boolean synchronous) {
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
        public IndexedChronicleQueueBuilder useCheckedExcerpt(boolean useCheckedExcerpt) {
            this.useCheckedExcerpt = useCheckedExcerpt;
            return this;
        }

        /**
         *
         * @return true if useCheckedExcerpt mode is enabled, false otherwise.
         */
        public boolean useCheckedExcerpt() {
            return this.useCheckedExcerpt;
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

        public ReplicaChronicleQueueBuilder sink() {
            return new SinkChronicleQueueBuilder(this);
        }

        public ReplicaChronicleQueueBuilder source() {
            return new SourceChronicleQueueBuilder(this);
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

    public static class VanillaChronicleQueueBuilder extends ChronicleQueueBuilder {
        private final File path;

        private boolean synchronous;
        private boolean useCheckedExcerpt;

        private String cycleFormat;
        private int cycleLength;
        private int defaultMessageSize;
        private int dataCacheCapacity;
        private int indexCacheCapacity;
        private long indexBlockSize;
        private long dataBlockSize;
        private long entriesPerCycle;
        private boolean cleanupOnClose;

        private VanillaChronicleQueueBuilder(File path) {
            this.path  = path;
            this.synchronous = false;
            this.useCheckedExcerpt = false;
            this.cycleFormat = "yyyyMMdd";
            this.cycleLength = 24 * 60 * 60 * 1000; // MILLIS_PER_DAY
            this.defaultMessageSize = 128 << 10; // 128 KB.
            this.dataCacheCapacity = 32;
            this.indexCacheCapacity = 32;
            this.indexBlockSize = 16L << 20; // 16 MB
            this.dataBlockSize = 64L << 20; // 64 MB
            this.entriesPerCycle = 1L << 40; // one trillion per day or per hour.
            this.cleanupOnClose = false;
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
        public VanillaChronicleQueueBuilder synchronous(boolean synchronous) {
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
        public VanillaChronicleQueueBuilder useCheckedExcerpt(boolean useCheckedExcerpt) {
            this.useCheckedExcerpt = useCheckedExcerpt;
            return this;
        }

        /**
         *
         * @return true if useCheckedExcerpt mode is enabled, false otherwise.
         */
        public boolean useCheckedExcerpt() {
            return this.useCheckedExcerpt;
        }

        public VanillaChronicleQueueBuilder cycleFormat(String cycleFormat) {
            this.cycleFormat = cycleFormat;
            return this;
        }

        public String cycleFormat() {
            return cycleFormat;
        }

        public VanillaChronicleQueueBuilder cycleLength(int cycleLength) {
            return cycleLength(cycleLength, true);
        }

        public VanillaChronicleQueueBuilder cycleLength(int cycleLength, boolean check) {
            if (check && cycleLength < VanillaChronicle.MIN_CYCLE_LENGTH) {
                throw new IllegalArgumentException(
                    "Cycle length can't be less than " + VanillaChronicle.MIN_CYCLE_LENGTH + " ms!");
            }

            this.cycleLength = cycleLength;
            return this;
        }

        public int cycleLength() {
            return cycleLength;
        }

        public VanillaChronicleQueueBuilder indexBlockSize(long indexBlockSize) {
            this.indexBlockSize = indexBlockSize;
            return this;
        }

        public long indexBlockSize() {
            return indexBlockSize;
        }

        public long dataBlockSize() {
            return dataBlockSize;
        }

        public VanillaChronicleQueueBuilder dataBlockSize(int dataBlockSize) {
            this.dataBlockSize = dataBlockSize;
            return this;
        }

        public VanillaChronicleQueueBuilder entriesPerCycle(long entriesPerCycle) {
            if(entriesPerCycle < 256) {
                throw new IllegalArgumentException("EntriesPerCycle must be at least 256");
            }

            if(entriesPerCycle > 1L << 48) {
                throw new IllegalArgumentException("EntriesPerCycle must not exceed 1L << 48 (" + (1L << 48) + ")");
            }

            if(!((entriesPerCycle & -entriesPerCycle) == entriesPerCycle)) {
                throw new IllegalArgumentException("EntriesPerCycle must be a power of 2");
            }

            this.entriesPerCycle = entriesPerCycle;
            return this;
        }

        public long entriesPerCycle() {
            return entriesPerCycle;
        }

        public VanillaChronicleQueueBuilder defaultMessageSize(int defaultMessageSize) {
            this.defaultMessageSize = defaultMessageSize;
            return this;
        }

        public int defaultMessageSize() {
            return defaultMessageSize;
        }

        public VanillaChronicleQueueBuilder cleanupOnClose(boolean cleanupOnClose) {
            this.cleanupOnClose = cleanupOnClose;
            return this;
        }

        public boolean cleanupOnClose() {
            return cleanupOnClose;
        }

        public VanillaChronicleQueueBuilder dataCacheCapacity(int dataCacheCapacity) {
            this.dataCacheCapacity = dataCacheCapacity;
            return this;
        }

        public int dataCacheCapacity() {
            return this.dataCacheCapacity;
        }

        public VanillaChronicleQueueBuilder indexCacheCapacity(int indexCacheCapacity) {
            this.indexCacheCapacity = indexCacheCapacity;
            return this;
        }

        public int indexCacheCapacity() {
            return this.indexCacheCapacity;
        }

        public ReplicaChronicleQueueBuilder sink() {
            return new SinkChronicleQueueBuilder(this);
        }

        public ReplicaChronicleQueueBuilder source() {
            return new SourceChronicleQueueBuilder(this);
        }

        @Override
        public Chronicle build() throws IOException {
            return new VanillaChronicle(this);
        }

        /**
         * Makes VanillaChronicleQueueBuilder cloneable.
         *
         * @return a cloned copy of this VanillaChronicleQueueBuilder instance
         */
        @NotNull
        @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
        @Override
        public VanillaChronicleQueueBuilder clone() {
            try {
                return (VanillaChronicleQueueBuilder) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new AssertionError(e);
            }
        }
    }

    // *************************************************************************
    //
    // *************************************************************************

    public static abstract class ReplicaChronicleQueueBuilder extends ChronicleQueueBuilder {
        private final ChronicleQueueBuilder builder;
        private Chronicle chronicle;

        private InetSocketAddress bindAddress;
        private InetSocketAddress connectAddress;
        private long reconnectTimeout;
        private TimeUnit reconnectTimeoutUnit;
        private long selectTimeout;
        private TimeUnit selectTimeoutUnit;
        private int receiveBufferSize;
        private int minBufferSize;
        private boolean sharedChronicle;
        private long heartbeatInterval;
        private TimeUnit heartbeatIntervalUnit;
        private int maxExcerptsPerMessage;
        private int selectorSpinLoopCount;

        private int acceptorMaxBacklog;
        private int acceptorDefaultThreads;
        private int acceptorMaxThreads;
        private long acceptorThreadPoolkeepAliveTime;
        private TimeUnit acceptorThreadPoolkeepAliveTimeUnit;

        private ReplicaChronicleQueueBuilder(Chronicle chronicle, ChronicleQueueBuilder builder) {
            this.builder = builder;
            this.chronicle = chronicle;

            this.bindAddress = null;
            this.connectAddress = null;
            this.reconnectTimeout = 500;
            this.reconnectTimeoutUnit = TimeUnit.MILLISECONDS;
            this.selectTimeout = 1000;
            this.selectTimeoutUnit = TimeUnit.MILLISECONDS;
            this.heartbeatInterval = 2500;
            this.heartbeatIntervalUnit = TimeUnit.MILLISECONDS;
            this.receiveBufferSize = 256 * 1024;
            this.minBufferSize = this.receiveBufferSize;
            this.sharedChronicle = false;
            this.acceptorMaxBacklog = 50;
            this.acceptorDefaultThreads = 0;
            this.acceptorMaxThreads = Integer.MAX_VALUE;
            this.acceptorThreadPoolkeepAliveTime = 60L;
            this.acceptorThreadPoolkeepAliveTimeUnit = TimeUnit.SECONDS;
            this.maxExcerptsPerMessage = 128;
            this.selectorSpinLoopCount = 100000;
        }

        public InetSocketAddress bindAddress() {
            return bindAddress;
        }

        public ReplicaChronicleQueueBuilder bindAddress(InetSocketAddress bindAddress) {
            this.bindAddress = bindAddress;
            return this;
        }

        public ReplicaChronicleQueueBuilder bindAddress(int port) {
            return bindAddress(new InetSocketAddress(port));
        }

        public ReplicaChronicleQueueBuilder bindAddress(String host, int port) {
            return bindAddress(new InetSocketAddress(host, port));
        }

        public InetSocketAddress connectAddress() {
            return connectAddress;
        }

        public ReplicaChronicleQueueBuilder connectAddress(InetSocketAddress connectAddress) {
            this.connectAddress = connectAddress;
            return this;
        }

        public ReplicaChronicleQueueBuilder connectAddress(String host, int port) {
            return connectAddress(new InetSocketAddress(host, port));
        }

        public long reconnectTimeout() {
            return reconnectTimeout;
        }

        public long reconnectTimeoutMillis() {
            return this.reconnectTimeoutUnit.toMillis(this.reconnectTimeout);
        }

        public ReplicaChronicleQueueBuilder reconnectTimeout(long reconnectTimeout, TimeUnit reconnectTimeoutUnit) {
            this.reconnectTimeout = reconnectTimeout;
            this.reconnectTimeoutUnit = reconnectTimeoutUnit;
            return this;
        }

        public TimeUnit getReconnectTimeoutUnit() {
            return this.reconnectTimeoutUnit;
        }

        public long selectTimeout() {
            return selectTimeout;
        }

        public long selectTimeoutMillis() {
            return this.selectTimeoutUnit.toMillis(this.selectTimeout);
        }

        public ReplicaChronicleQueueBuilder selectTimeout(long selectTimeout, TimeUnit selectTimeoutUnit) {
            this.selectTimeout = selectTimeout;
            this.selectTimeoutUnit = selectTimeoutUnit;
            return this;
        }

        public TimeUnit selectTimeoutUnit() {
            return this.selectTimeoutUnit;
        }

        public ReplicaChronicleQueueBuilder heartbeatInterval(long heartbeatInterval, TimeUnit heartbeatIntervalUnit) {
            this.heartbeatInterval = heartbeatInterval;
            this.heartbeatIntervalUnit = heartbeatIntervalUnit;
            return this;
        }

        public long heartbeatInterval() {
            return this.heartbeatInterval;
        }

        public long heartbeatIntervalMillis() {
            return this.heartbeatIntervalUnit.toMillis(this.heartbeatInterval);
        }

        public TimeUnit heartbeatIntervalUnit() {
            return this.heartbeatIntervalUnit;
        }

        public int receiveBufferSize() {
            return receiveBufferSize;
        }

        public ReplicaChronicleQueueBuilder receiveBufferSize(int receiveBufferSize) {
            this.receiveBufferSize = receiveBufferSize;
            return this;
        }

        public int minBufferSize() {
            return minBufferSize;
        }

        public ReplicaChronicleQueueBuilder minBufferSize(int minBufferSize) {
            this.minBufferSize = minBufferSize;
            return this;
        }

        public int maxExcerptsPerMessage() {
            return this.maxExcerptsPerMessage;
        }

        public ReplicaChronicleQueueBuilder maxExcerptsPerMessage(int maxExcerptsPerMessage) {
            this.maxExcerptsPerMessage = maxExcerptsPerMessage;
            return this;
        }

        public boolean sharedChronicle() {
            return this.sharedChronicle;
        }

        public ReplicaChronicleQueueBuilder sharedChronicle(boolean sharedChronicle) {
            this.sharedChronicle = sharedChronicle;
            return this;
        }

        public ReplicaChronicleQueueBuilder acceptorMaxBacklog(int acceptorMaxBacklog) {
            this.acceptorMaxBacklog = acceptorMaxBacklog;
            return this;
        }

        public int acceptorMaxBacklog() {
            return this.acceptorMaxBacklog;
        }

        public ReplicaChronicleQueueBuilder acceptorDefaultThreads(int acceptorDefaultThreads) {
            this.acceptorDefaultThreads = acceptorDefaultThreads;
            return this;
        }

        public int acceptorDefaultThreads() {
            return this.acceptorDefaultThreads;
        }

        public ReplicaChronicleQueueBuilder acceptorMaxThreads(int acceptorMaxThreads) {
            this.acceptorMaxThreads = acceptorMaxThreads;
            return this;
        }

        public int acceptorMaxThreads() {
            return this.acceptorMaxThreads;
        }

        public ReplicaChronicleQueueBuilder acceptorThreadPoolkeepAlive(long acceptorThreadPoolkeepAlive, TimeUnit acceptorThreadPoolkeepAliveUnit) {
            this.acceptorThreadPoolkeepAliveTime = acceptorThreadPoolkeepAlive;
            this.acceptorThreadPoolkeepAliveTimeUnit = acceptorThreadPoolkeepAliveUnit;
            return this;
        }

        public long acceptorThreadPoolkeepAliveTime() {
            return this.acceptorThreadPoolkeepAliveTime;
        }

        public TimeUnit acceptorThreadPoolkeepAliveTimeUnit() {
            return this.acceptorThreadPoolkeepAliveTimeUnit;
        }

        public long acceptorThreadPoolkeepAliveTimeMillis() {
            return this.acceptorThreadPoolkeepAliveTimeUnit.toMillis(this.acceptorThreadPoolkeepAliveTime);
        }

        public int selectorSpinLoopCount() {
            return this.selectorSpinLoopCount;
        }

        public ReplicaChronicleQueueBuilder selectorSpinLoopCount(int selectorSpinLoopCount) {
            this.selectorSpinLoopCount = selectorSpinLoopCount;
            return this;
        }

        public Chronicle chronicle() {
            return this.chronicle;
        }

        @Override
        public Chronicle build() throws IOException {
            if(this.builder != null) {
                this.chronicle = this.builder.build();
            }

            return doBuild();
        }

        protected abstract Chronicle doBuild() throws IOException;

        /**
         * Makes ReplicaChronicleQueueBuilder cloneable.
         *
         * @return a cloned copy of this ReplicaChronicleQueueBuilder instance
         */
        @NotNull
        @Override
        public ReplicaChronicleQueueBuilder clone() throws CloneNotSupportedException{
            return (ReplicaChronicleQueueBuilder) super.clone();
        }
    }

    private static class SinkChronicleQueueBuilder extends ReplicaChronicleQueueBuilder {

        private SinkChronicleQueueBuilder() {
            super(null, null);
        }

        private SinkChronicleQueueBuilder(@NotNull ChronicleQueueBuilder builder) {
            super(null, builder);
        }

        private SinkChronicleQueueBuilder(@NotNull Chronicle chronicle) {
            super(chronicle, null);
        }

        @Override
        public Chronicle doBuild() throws IOException {
            SinkTcp cnx;

            if(bindAddress() != null && connectAddress() == null) {
                cnx = new SinkTcpAcceptor(this);
            } else if(connectAddress() != null) {
                cnx = new SinkTcpInitiator(this);
            } else {
                throw new IllegalArgumentException("BindAddress and ConnectAddress are not set");
            }

            return new ChronicleSink(this, cnx);
        }

        /**
         * Makes SinkChronicleQueueBuilder cloneable.
         *
         * @return a cloned copy of this SinkChronicleQueueBuilder instance
         */
        @NotNull
        @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
        @Override
        public SinkChronicleQueueBuilder clone() {
            try {
                return (SinkChronicleQueueBuilder) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new AssertionError(e);
            }
        }
    }

    private static class SourceChronicleQueueBuilder extends ReplicaChronicleQueueBuilder {

        private SourceChronicleQueueBuilder(@NotNull ChronicleQueueBuilder builder) {
            super(null, builder);
        }

        private SourceChronicleQueueBuilder(@NotNull Chronicle chronicle) {
            super(chronicle, null);
        }

        @Override
        public Chronicle doBuild() throws IOException {
            SourceTcp cnx;

            if(bindAddress() != null && connectAddress() == null) {
                cnx = new SourceTcpAcceptor(this);
            } else if(connectAddress() != null) {
                cnx = new SourceTcpInitiator(this);
            } else {
                throw new IllegalArgumentException("BindAddress and ConnectAddress are not set");
            }

            return new ChronicleSource(this, cnx);
        }

        /**
         * Makes SinkChronicleQueueBuilder cloneable.
         *
         * @return a cloned copy of this SinkChronicleQueueBuilder instance
         */
        @NotNull
        @SuppressWarnings("SourceChronicleQueueBuilder")
        @Override
        public SourceChronicleQueueBuilder clone() {
            try {
                return (SourceChronicleQueueBuilder) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new AssertionError(e);
            }
        }
    }

    // *************************************************************************
    //
    // *************************************************************************

    /**
     * Makes ChronicleQueueBuilder cloneable.
     *
     * @return a cloned copy of this ChronicleQueueBuilder instance
     */
    @Override
    public ChronicleQueueBuilder clone() throws CloneNotSupportedException {
        return (ChronicleQueueBuilder) super.clone();
    }
}
