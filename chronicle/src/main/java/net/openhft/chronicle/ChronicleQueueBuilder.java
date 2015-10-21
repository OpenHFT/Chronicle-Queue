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
import net.openhft.lang.Maths;
import net.openhft.lang.io.FileLifecycleListener;
import net.openhft.lang.model.constraints.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public abstract class ChronicleQueueBuilder implements Cloneable {

    public static IndexedChronicleQueueBuilder indexed(File path) {
        return new IndexedChronicleQueueBuilder(path);
    }

    // *************************************************************************
    //
    // *************************************************************************

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

    public static ReplicaChronicleQueueBuilder sink(Chronicle chronicle) {
        return new SinkChronicleQueueBuilder(chronicle);
    }

    public static ReplicaChronicleQueueBuilder source(Chronicle chronicle) {
        return new SourceChronicleQueueBuilder(chronicle);
    }

    public static ReplicaChronicleQueueBuilder remoteAppender() {
        return new RemoteChronicleQueueAppenderBuilder();
    }

    public static ReplicaChronicleQueueBuilder remoteTailer() {
        return new RemoteChronicleQueueTailerBuilder();
    }

    public abstract Chronicle build() throws IOException;

    // *************************************************************************
    //
    // *************************************************************************

    /**
     * Makes ChronicleQueueBuilder cloneable.
     *
     * @return a cloned copy of this ChronicleQueueBuilder instance
     */
    @NotNull
    @SuppressWarnings("SourceChronicleQueueBuilder")
    @Override
    public ChronicleQueueBuilder clone() throws CloneNotSupportedException {
        return (ChronicleQueueBuilder) super.clone();
    }

    public static class IndexedChronicleQueueBuilder extends ChronicleQueueBuilder implements Cloneable {

        private final File path;

        private boolean synchronous;
        private boolean useCheckedExcerpt;
        private boolean useCompressedObjectSerializer;

        private int cacheLineSize;
        private int dataBlockSize;
        private int messageCapacity;
        private int indexBlockSize;

        private FileLifecycleListener fileLifecycleListener = FileLifecycleListener.FileLifecycleListeners.IGNORE;

        /**
         * On 64 bit JVMs it has the following params: <ul> <li>data block size <b>128M</b></li>
         * </ul>
         *
         * On 32 bit JVMs it has the following params: <ul> <li>data block size <b>16M</b></li>
         * </ul>
         */
        private IndexedChronicleQueueBuilder(final File path) {
            this.path = path;
            this.synchronous = false;
            this.useCheckedExcerpt = false;
            this.useCompressedObjectSerializer = true;
            this.cacheLineSize = 64;
            this.dataBlockSize = Jvm.is64Bit() ? 128 * 1024 * 1024 : 16 * 1024 * 1024;
            this.indexBlockSize = Math.max(4096, this.dataBlockSize / 4);
            this.messageCapacity = 128 * 1024;
        }

        protected File path() {
            return this.path;
        }

        /**
         * Sets the synchronous mode to be used. Enabling synchronous mode means that {@link
         * ExcerptCommon#finish()} will force a persistence every time.
         *
         * @param synchronous If synchronous mode should be used or not.
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
         * @param useCheckedExcerpt
         * @return this builder object back
         */
        public IndexedChronicleQueueBuilder useCheckedExcerpt(boolean useCheckedExcerpt) {
            this.useCheckedExcerpt = useCheckedExcerpt;
            return this;
        }

        /**
         * @return true if useCheckedExcerpt mode is enabled, false otherwise.
         */
        public boolean useCheckedExcerpt() {
            return this.useCheckedExcerpt;
        }

        public boolean useCompressedObjectSerializer() {
            return this.useCompressedObjectSerializer;
        }

        public IndexedChronicleQueueBuilder useCompressedObjectSerializer(boolean useCompressedObjectSerializer) {
            this.useCompressedObjectSerializer = useCompressedObjectSerializer;
            return this;
        }

        /**
         * Sets the size of the index cache lines. Index caches (files) consist of fixed size lines,
         * each line having ultiple index entries on it. This param specifies the size of such a
         * multi entry line.
         *
         * Default value is <b>64</b>.
         *
         * @param cacheLineSize the size of the cache lines making up index files
         * @return this builder object back
         */
        public IndexedChronicleQueueBuilder cacheLineSize(int cacheLineSize) {
            this.cacheLineSize = cacheLineSize;
            return this;
        }

        /**
         * The size of the index cache lines (index caches are made up of multiple fixed length
         * lines, each line contains multiple index entries).
         *
         * Default value is <b>64</b>.
         *
         * @return the size of the index cache lines
         */
        public int cacheLineSize() {
            return this.cacheLineSize;
        }

        /**
         * Sets the size to be used for data blocks. The method also has a side effect. If the data
         * block size specified is smaller than twice the message capacity, then the message
         * capacity will be adjusted to equal half of the new data block size.
         *
         * @param dataBlockSize the size of the data blocks
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
         * Sets the size to be used for index blocks. Is capped to the bigger value among
         * <b>4096</b> and a <b>quarter of the <tt>data block size</tt></b>.
         *
         * @param indexBlockSize the size of the index blocks
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
         * The maximum size a message stored in a {@link net.openhft.chronicle.Chronicle} instance
         * can have. Defaults to <b>128K</b>. Is limited by the <tt>data block size</tt>, can't be
         * bigger than half of the data block size.
         *
         * @param messageCapacity the maximum message size that can be stored
         * @return tthis builder object back
         */
        public IndexedChronicleQueueBuilder messageCapacity(int messageCapacity) {
            this.messageCapacity = messageCapacity;
            return this;
        }

        /**
         * The maximum size of the message that can be stored in the {@link
         * net.openhft.chronicle.Chronicle} instance to be configured. Defaults to <b>128K</b>.
         * Can't be bigger than half of the <tt>data block size</tt>.
         *
         * @return the maximum message size that can be stored
         */
        public int messageCapacity() {
            return this.messageCapacity;
        }

        /**
         * A pre-defined ChronicleBuilder for small {@link net.openhft.chronicle.Chronicle}
         * instances.
         *
         * It has the following params: <ul> <li>data block size <b>16M</b></li> </ul>
         */
        public IndexedChronicleQueueBuilder small() {
            dataBlockSize(16 * 1024 * 1024);
            return this;
        }

        /**
         * A pre-defined ChronicleBuilder for medium {@link net.openhft.chronicle.Chronicle}
         * instances.
         *
         * It has the following params: <ul> <li>data block size <b>128M</b></li> </ul>
         */
        public IndexedChronicleQueueBuilder medium() {
            dataBlockSize(128 * 1024 * 1024);
            return this;
        }

        /**
         * A pre-defined ChronicleBuilder for large {@link net.openhft.chronicle.Chronicle}
         * instances.
         *
         * It has the following params: <ul> <li>data block size <b>512M</b></li> </ul>
         */
        public IndexedChronicleQueueBuilder large() {
            dataBlockSize(512 * 1024 * 1024);
            return this;
        }

        /**
         * A pre-defined ChronicleBuilder for test {@link net.openhft.chronicle.Chronicle}
         * instances.
         *
         * It has the following params: <ul> <li>data block size <b>8k</b></li> </ul>
         */
        IndexedChronicleQueueBuilder test() {
            dataBlockSize(8 * 1024);
            return this;
        }

        public ReplicaChronicleQueueBuilder sink() {
            return new SinkChronicleQueueBuilder(this);
        }

        public ReplicaChronicleQueueBuilder source() {
            return new SourceChronicleQueueBuilder(this);
        }

        public IndexedChronicleQueueBuilder fileGrowthListener(FileLifecycleListener fileLifecycleListener) {
            this.fileLifecycleListener = fileLifecycleListener;
            return this;
        }

        public FileLifecycleListener fileLifecycleListener() {
            return fileLifecycleListener;
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

    // *************************************************************************
    //
    // *************************************************************************

    public static class VanillaChronicleQueueBuilder extends ChronicleQueueBuilder {
        private final File path;

        private boolean synchronous;
        private boolean syncOnRoll;
        private boolean useCheckedExcerpt;
        private boolean useCompressedObjectSerializer;

        private String cycleFormat;
        private int cycleLength;
        private TimeZone cycleTimeZone;

        private int defaultMessageSize;
        private int dataCacheCapacity;
        private int indexCacheCapacity;
        private long indexBlockSize;
        private long dataBlockSize;
        private long entriesPerCycle;
        private boolean cleanupOnClose;
        private FileLifecycleListener fileLifecycleListener = FileLifecycleListener.FileLifecycleListeners.IGNORE;

        private VanillaChronicleQueueBuilder(File path) {
            this.path = path;
            this.synchronous = false;
            this.syncOnRoll = true;
            this.useCheckedExcerpt = false;
            this.defaultMessageSize = 128 << 10; // 128 KB.
            this.dataCacheCapacity = 32;
            this.indexCacheCapacity = 32;
            this.indexBlockSize = 16L << 20; // 16 MB
            this.dataBlockSize = 64L << 20; // 64 MB
            this.cycleFormat = VanillaChronicle.Cycle.DAYS.format();
            this.cycleLength = VanillaChronicle.Cycle.DAYS.length();
            this.entriesPerCycle = VanillaChronicle.Cycle.DAYS.entries();
            this.cycleTimeZone = TimeZone.getTimeZone("GMT");
            this.cleanupOnClose = false;
            this.useCompressedObjectSerializer = true;
        }

        protected File path() {
            return this.path;
        }

        /**
         * Sets the synchronous mode to be used. Enabling synchronous mode means that {@link
         * ExcerptCommon#finish()} will force a persistence every time.
         *
         * @param synchronous If synchronous mode should be used or not.
         * @return this builder object back
         */
        public VanillaChronicleQueueBuilder synchronous(boolean synchronous) {
            this.synchronous = synchronous;
            return this;
        }

        public boolean syncOnRoll() {
            return this.syncOnRoll;
        }

        public VanillaChronicleQueueBuilder syncOnRoll(boolean syncOnRoll) {
            this.syncOnRoll = syncOnRoll;
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
         * @param useCheckedExcerpt
         * @return this builder object back
         */
        public VanillaChronicleQueueBuilder useCheckedExcerpt(boolean useCheckedExcerpt) {
            this.useCheckedExcerpt = useCheckedExcerpt;
            return this;
        }

        /**
         * @return true if useCheckedExcerpt mode is enabled, false otherwise.
         */
        public boolean useCheckedExcerpt() {
            return this.useCheckedExcerpt;
        }

        public boolean useCompressedObjectSerializer() {
            return this.useCompressedObjectSerializer;
        }

        public VanillaChronicleQueueBuilder useCompressedObjectSerializer(boolean useCompressedObjectSerializer) {
            this.useCompressedObjectSerializer = useCompressedObjectSerializer;
            return this;
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
            entriesPerCycle(VanillaChronicle.Cycle.forLength(this.cycleLength).entries());

            return this;
        }

        public int cycleLength() {
            return cycleLength;
        }

        public VanillaChronicleQueueBuilder cycleTimeZone(String timeZone) {
            this.cycleTimeZone = TimeZone.getTimeZone(timeZone);
            return this;
        }

        public VanillaChronicleQueueBuilder cycleTimeZone(TimeZone timeZone) {
            this.cycleTimeZone = timeZone;
            return this;
        }

        public TimeZone cycleTimeZone() {
            return cycleTimeZone;
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

        public VanillaChronicleQueueBuilder dataBlockSize(long dataBlockSize) {
            this.dataBlockSize = dataBlockSize;
            return this;
        }

        public VanillaChronicleQueueBuilder cycle(VanillaChronicle.Cycle cycle) {
            cycleFormat(cycle.format());
            cycleLength(cycle.length(), false);
            entriesPerCycle(cycle.entries());
            return this;
        }

        public VanillaChronicleQueueBuilder entriesPerCycle(long entriesPerCycle) {
            if (entriesPerCycle < 256) {
                throw new IllegalArgumentException("EntriesPerCycle must be at least 256");
            }

            if (entriesPerCycle > 1L << 48) {
                throw new IllegalArgumentException("EntriesPerCycle must not exceed 1L << 48 (" + (1L << 48) + ")");
            }

            if(!Maths.isPowerOf2(entriesPerCycle)) {
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

        public VanillaChronicleQueueBuilder fileLifecycleListener(FileLifecycleListener fileLifecycleListener) {
            this.fileLifecycleListener = fileLifecycleListener;
            return this;
        }

        public FileLifecycleListener fileLifecycleListener() {
            return fileLifecycleListener;
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

    public static abstract class ReplicaChronicleQueueBuilder extends ChronicleQueueBuilder
            implements MappingProvider<ReplicaChronicleQueueBuilder> {

        public static final int DEFAULT_SOCKET_BUFFER_SIZE = 256 * 1024;
        public static final TcpConnectionListener CONNECTION_LISTENER = new TcpConnectionHandler();

        private final ChronicleQueueBuilder builder;
        private Chronicle chronicle;

        private AddressProvider bindAddressProvider;
        private AddressProvider connectAddressProvider;
        private int reconnectionAttempts;
        private int reconnectionWarningThreshold;
        private long reconnectionInterval;
        private TimeUnit reconnectionIntervalUnit;
        private long selectTimeout;
        private TimeUnit selectTimeoutUnit;
        private int receiveBufferSize;
        private int sendBufferSize;
        private int minBufferSize;
        private boolean sharedChronicle;
        private long heartbeatInterval;
        private TimeUnit heartbeatIntervalUnit;
        private int maxExcerptsPerMessage;
        private int selectorSpinLoopCount;
        private int readSpinCount;
        private boolean appendRequireAck;

        private int acceptorMaxBacklog;
        private int acceptorDefaultThreads;
        private int acceptorMaxThreads;
        private long acceptorThreadPoolKeepAliveTime;
        private TimeUnit acceptorThreadPoolKeepAliveTimeUnit;
        private TcpConnectionListener connectionListener;

        private long busyPeriod;
        private TimeUnit busyPeriodTimeUnit;
        private long parkPeriod;
        private TimeUnit parkPeriodTimeUnit;

        @Nullable
        private MappingFunction mapping;

        private ReplicaChronicleQueueBuilder(Chronicle chronicle, ChronicleQueueBuilder builder) {
            this.builder = builder;
            this.chronicle = chronicle;

            this.bindAddressProvider = null;
            this.connectAddressProvider = null;
            this.reconnectionInterval = 500;
            this.reconnectionIntervalUnit = TimeUnit.MILLISECONDS;
            this.reconnectionAttempts = 1;
            this.reconnectionWarningThreshold = 10;
            this.selectTimeout = 1000;
            this.selectTimeoutUnit = TimeUnit.MILLISECONDS;
            this.heartbeatInterval = 2500;
            this.heartbeatIntervalUnit = TimeUnit.MILLISECONDS;
            this.receiveBufferSize = DEFAULT_SOCKET_BUFFER_SIZE;
            this.sendBufferSize = DEFAULT_SOCKET_BUFFER_SIZE;
            this.minBufferSize = DEFAULT_SOCKET_BUFFER_SIZE;
            this.sharedChronicle = false;
            this.acceptorMaxBacklog = 50;
            this.acceptorDefaultThreads = 0;
            this.acceptorMaxThreads = Integer.MAX_VALUE;
            this.acceptorThreadPoolKeepAliveTime = 60L;
            this.acceptorThreadPoolKeepAliveTimeUnit = TimeUnit.SECONDS;
            this.maxExcerptsPerMessage = 128;
            this.selectorSpinLoopCount = 100000;
            this.readSpinCount = -1;
            this.connectionListener = CONNECTION_LISTENER;
            this.appendRequireAck = false;
            this.mapping = null;
            this.busyPeriod = TimeUnit.NANOSECONDS.convert(20, TimeUnit.MICROSECONDS);
            this.busyPeriodTimeUnit = TimeUnit.NANOSECONDS;
            this.parkPeriod = TimeUnit.NANOSECONDS.convert(200, TimeUnit.MICROSECONDS);
            this.parkPeriodTimeUnit = TimeUnit.NANOSECONDS;
        }

        public InetSocketAddress bindAddress() {
            return bindAddressProvider != null
                    ? bindAddressProvider.get()
                    : null;
        }

        public ReplicaChronicleQueueBuilder bindAddress(final InetSocketAddress bindAddress) {
            this.bindAddressProvider =  AddressProviders.single(bindAddress);
            return this;
        }

        public ReplicaChronicleQueueBuilder bindAddress(int port) {
            return bindAddress(new InetSocketAddress(port));
        }

        public ReplicaChronicleQueueBuilder bindAddress(String host, int port) {
            return bindAddress(new InetSocketAddress(host, port));
        }

        public AddressProvider bindAddressProvider() {
            return this.connectAddressProvider;
        }

        public ReplicaChronicleQueueBuilder bindAddressProvider(AddressProvider bindAddressProvider ) {
            this.bindAddressProvider = bindAddressProvider;
            return this;
        }

        public InetSocketAddress connectAddress() {
            return connectAddressProvider != null
                    ? connectAddressProvider.get()
                    : null;
        }

        public ReplicaChronicleQueueBuilder connectAddress(final InetSocketAddress connectAddress) {
            this.connectAddressProvider = AddressProviders.single(connectAddress);
            return this;
        }

        public AddressProvider connectAddressProvider() {
            return this.connectAddressProvider;
        }

        public ReplicaChronicleQueueBuilder connectAddressProvider(AddressProvider connectAddressprovider ) {
            this.connectAddressProvider = connectAddressprovider;
            return this;
        }

        public ReplicaChronicleQueueBuilder connectAddress(String host, int port) {
            return connectAddress(new InetSocketAddress(host, port));
        }

        public long reconnectionreconnectionInterval() {
            return reconnectionInterval;
        }

        public ReplicaChronicleQueueBuilder reconnectionInterval(long reconnectTimeout) {
            this.reconnectionInterval = reconnectTimeout;
            return this;
        }

        public long reconnectionIntervalMillis() {
            return this.reconnectionIntervalUnit.toMillis(this.reconnectionInterval);
        }

        public ReplicaChronicleQueueBuilder reconnectionInterval(long reconnectionInterval, TimeUnit reconnectionIntervalUnit) {
            this.reconnectionInterval = reconnectionInterval;
            this.reconnectionIntervalUnit = reconnectionIntervalUnit;
            return this;
        }

        public TimeUnit reconnectionIntervalUnit() {
            return this.reconnectionIntervalUnit;
        }

        public ReplicaChronicleQueueBuilder reconnectionIntervalUnit(TimeUnit reconnectionInterval) {
            this.reconnectionIntervalUnit = reconnectionInterval;
            return this;
        }

        public int reconnectionAttempts() {
            return this.reconnectionAttempts;
        }

        public ReplicaChronicleQueueBuilder reconnectionAttempts(int reconnectionAttempts) {
            if(reconnectionAttempts < 0) {
                throw new IllegalArgumentException("ReconnectionAttempts must be greater or equals than 0");
            }

            this.reconnectionAttempts = reconnectionAttempts;
            return this;
        }

        public int reconnectionWarningThreshold() {
            return this.reconnectionWarningThreshold;
        }

        public ReplicaChronicleQueueBuilder reconnectionWarningThreshold(int reconnectionWarningThreshold) {
            if(reconnectionWarningThreshold < 0) {
                throw new IllegalArgumentException("ReconnectionWarningThreshold must be greater or equals than 0");
            }

            this.reconnectionWarningThreshold = reconnectionWarningThreshold;
            return this;
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

        public int defaultSocketBufferSize() {
            return DEFAULT_SOCKET_BUFFER_SIZE;
        }

        public int receiveBufferSize() {
            return receiveBufferSize;
        }

        public ReplicaChronicleQueueBuilder receiveBufferSize(int receiveBufferSize) {
            this.receiveBufferSize = receiveBufferSize;
            return this;
        }

        public int sendBufferSize() {
            return sendBufferSize;
        }

        public ReplicaChronicleQueueBuilder sendBufferSize(int sendBufferSize) {
            this.sendBufferSize = sendBufferSize;
            return this;
        }

        public int minBufferSize() {
            return minBufferSize;
        }

        public ReplicaChronicleQueueBuilder minBufferSize(int minBufferSize) {
            if(!Maths.isPowerOf2(minBufferSize)) {
                throw new IllegalArgumentException("MinBufferSize must be a power of 2");
            }

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

        public ReplicaChronicleQueueBuilder acceptorThreadPoolkeepAlive(long acceptorThreadPoolKeepAliveTime, TimeUnit acceptorThreadPoolKeepAliveTimeUnit) {
            this.acceptorThreadPoolKeepAliveTime = acceptorThreadPoolKeepAliveTime;
            this.acceptorThreadPoolKeepAliveTimeUnit = acceptorThreadPoolKeepAliveTimeUnit;
            return this;
        }

        public long acceptorThreadPoolkeepAliveTime() {
            return this.acceptorThreadPoolKeepAliveTime;
        }

        public ReplicaChronicleQueueBuilder acceptorThreadPoolkeepAliveTime(long acceptorThreadPoolKeepAliveTime) {
            this.acceptorThreadPoolKeepAliveTime = acceptorThreadPoolKeepAliveTime;
            return this;
        }

        public TimeUnit acceptorThreadPoolkeepAliveTimeUnit() {
            return this.acceptorThreadPoolKeepAliveTimeUnit;
        }

        public ReplicaChronicleQueueBuilder acceptorThreadPoolkeepAliveTimeUnit(TimeUnit acceptorThreadPoolKeepAliveTimeUnit) {
            this.acceptorThreadPoolKeepAliveTimeUnit = acceptorThreadPoolKeepAliveTimeUnit;
            return this;
        }

        public long acceptorThreadPoolkeepAliveTimeMillis() {
            return this.acceptorThreadPoolKeepAliveTimeUnit.toMillis(this.acceptorThreadPoolKeepAliveTime);
        }

        public ReplicaChronicleQueueBuilder busyPeriod(long busyPeriod, TimeUnit busyPeriodTimeUnit) {
            this.busyPeriod = busyPeriod;
            this.busyPeriodTimeUnit = busyPeriodTimeUnit;
            return this;
        }

        public long busyPeriod() {
            return this.busyPeriod;
        }

        public ReplicaChronicleQueueBuilder busyPeriod(long busyPeriod) {
            this.busyPeriod = busyPeriod;
            return this;
        }

        public TimeUnit busyPeriodTimeUnit() {
            return this.busyPeriodTimeUnit;
        }

        public ReplicaChronicleQueueBuilder busyPeriodTimeUnit(TimeUnit busyPeriodTimeUnit) {
            this.busyPeriodTimeUnit = busyPeriodTimeUnit;
            return this;
        }

        public long busyPeriodTimeNanos() {
            return this.busyPeriodTimeUnit.toNanos(this.busyPeriod);
        }

        public ReplicaChronicleQueueBuilder parkPeriod(long parkPeriod, TimeUnit parkPeriodTimeUnit) {
            this.parkPeriod = parkPeriod;
            this.parkPeriodTimeUnit = parkPeriodTimeUnit;
            return this;
        }

        public long parkPeriod() {
            return this.parkPeriod;
        }

        public ReplicaChronicleQueueBuilder parkPeriod(long parkPeriod) {
            this.parkPeriod = parkPeriod;
            return this;
        }

        public TimeUnit parkPeriodTimeUnit() {
            return this.parkPeriodTimeUnit;
        }

        public ReplicaChronicleQueueBuilder parkPeriodTimeUnit(TimeUnit parkPeriodTimeUnit) {
            this.parkPeriodTimeUnit = parkPeriodTimeUnit;
            return this;
        }

        public long parkPeriodTimeNanos() {
            return this.parkPeriodTimeUnit.toNanos(this.parkPeriod);
        }

        public int selectorSpinLoopCount() {
            return this.selectorSpinLoopCount;
        }

        public ReplicaChronicleQueueBuilder selectorSpinLoopCount(int selectorSpinLoopCount) {
            if(selectorSpinLoopCount != -1 && selectorSpinLoopCount <= 0) {
                throw new IllegalArgumentException("SelectorSpinLoopCount must be greater than 0 or -1 (disabled)");
            }

            this.selectorSpinLoopCount = selectorSpinLoopCount;
            return this;
        }

        public int readSpinCount() {
            return this.readSpinCount;
        }

        public ReplicaChronicleQueueBuilder readSpinCount(int readSpinCount) {
            if(readSpinCount != -1 && readSpinCount <= 0) {
                throw new IllegalArgumentException("ReadSpinCount must be greater than 0 or -1 (disabled)");
            }

            this.readSpinCount = readSpinCount;

            return this;
        }

        public ReplicaChronicleQueueBuilder connectionListener(TcpConnectionListener connectionListener) {
            this.connectionListener = connectionListener;
            return this;
        }

        public boolean hasConnectionListener() {
            return this.connectionListener != null;
        }

        public TcpConnectionListener connectionListener() {
            return this.connectionListener;
        }

        public ReplicaChronicleQueueBuilder appendRequireAck(boolean appendRequireAck) {
            this.appendRequireAck = appendRequireAck;
            return this;
        }

        public boolean appendRequireAck() {
            return this.appendRequireAck;
        }

        public Chronicle chronicle() {
            return this.chronicle;
        }

        @Override
        public Chronicle build() throws IOException {
            if (this.builder != null) {
                this.chronicle = this.builder.build();
            }

            return doBuild();
        }

        /**
         * the {@code mapping} is send from the sink to the source ( usually via TCP/IP ) this
         * mapping is then applied to the the source before the data is sent to the sink
         *
         * @param mapping a mapping function which is sent to the soure
         * @return this object
         */
        public ReplicaChronicleQueueBuilder withMapping(@Nullable MappingFunction mapping) {
            this.mapping = mapping;
            return this;
        }

        public MappingFunction withMapping() {
            return this.mapping;
        }

        protected abstract Chronicle doBuild();

        /**
         * Makes ReplicaChronicleQueueBuilder cloneable.
         *
         * @return a cloned copy of this ReplicaChronicleQueueBuilder instance
         */
        @NotNull
        @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
        @Override
        public ReplicaChronicleQueueBuilder clone() {
            try {
                return (ReplicaChronicleQueueBuilder) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new AssertionError(e);
            }
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
        public Chronicle doBuild() {
            SinkTcp cnx;

            if (bindAddress() != null && connectAddress() == null) {
                cnx = new SinkTcpAcceptor(this);

            } else if (connectAddress() != null) {
                cnx = new SinkTcpInitiator(this);

            } else {
                throw new IllegalArgumentException("BindAddress and ConnectAddress are not set");
            }

            return new ChronicleQueueSink(this, cnx);
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
            return (SinkChronicleQueueBuilder) super.clone();
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
        public Chronicle doBuild() {
            SourceTcp cnx;

            if (bindAddress() != null && connectAddress() == null) {
                cnx = new SourceTcpAcceptor(this);

            } else if (connectAddress() != null) {
                cnx = new SourceTcpInitiator(this);

            } else {
                throw new IllegalArgumentException("BindAddress and ConnectAddress are not set");
            }

            return new ChronicleQueueSource(this, cnx);
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
            return (SourceChronicleQueueBuilder) super.clone();
        }
    }

    private static final class RemoteChronicleQueueAppenderBuilder extends ReplicaChronicleQueueBuilder {

        private RemoteChronicleQueueAppenderBuilder() {
            super(null, null);
        }

        @Override
        public Chronicle doBuild() {
            SinkTcp cnx;

            if(bindAddress() != null && connectAddress() == null) {
                cnx = new SinkTcpAcceptor(this);

            } else if(connectAddress() != null) {
                cnx = new SinkTcpInitiator(this);

            } else {
                throw new IllegalArgumentException("BindAddress and ConnectAddress are not set");
            }

            return new RemoteChronicleQueueAppender(this, cnx);
        }

        /**
         * Makes SinkChronicleQueueBuilder cloneable.
         *
         * @return a cloned copy of this SinkChronicleQueueBuilder instance
         */
        @NotNull
        @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
        @Override
        public RemoteChronicleQueueAppenderBuilder clone() {
            return (RemoteChronicleQueueAppenderBuilder) super.clone();
        }
    }

    // *************************************************************************
    //
    // *************************************************************************

    private static final class RemoteChronicleQueueTailerBuilder extends ReplicaChronicleQueueBuilder {

        private RemoteChronicleQueueTailerBuilder() {
            super(null, null);
        }

        @Override
        public Chronicle doBuild() {
            SinkTcp cnx;

            if(bindAddress() != null && connectAddress() == null) {
                cnx = new SinkTcpAcceptor(this);

            } else if(connectAddress() != null) {
                cnx = new SinkTcpInitiator(this);

            } else {
                throw new IllegalArgumentException("BindAddress and ConnectAddress are not set");
            }

            return new RemoteChronicleQueueTailer(this, cnx);
        }

        /**
         * Makes SinkChronicleQueueBuilder cloneable.
         *
         * @return a cloned copy of this SinkChronicleQueueBuilder instance
         */
        @NotNull
        @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
        @Override
        public RemoteChronicleQueueTailerBuilder clone() {
            return (RemoteChronicleQueueTailerBuilder) super.clone();
        }
    }
}
