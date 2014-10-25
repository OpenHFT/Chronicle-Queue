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


import java.io.File;
import java.net.InetSocketAddress;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;

public class ChronicleQueueBuilder {

    private final File path;
    private final ChronicleQueueType type;
    private final IndexedConfig indexedConfig;
    private final VanillaConfig vanillaConfig;
    private final ReplicaConfig replicaConfig;

    private boolean synchonous;
    private boolean useCheckedExcerpt;



    private ChronicleQueueBuilder(ChronicleQueueType type, File path) {
        this.type = type;
        this.path = path;
        this.synchonous = false;
        this.useCheckedExcerpt = false;
        this.indexedConfig = new IndexedConfig();
        this.vanillaConfig = new VanillaConfig();
        this.replicaConfig = new ReplicaConfig();
    }

    /**
     *
     * @param synchonous
     *
     * @return this builder object back
     */
    public ChronicleQueueBuilder synchonous(boolean synchonous) {
        this.synchonous = synchonous;
        return this;
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

    /**
     *
     * @param bindAddress
     *
     * @return this builder object back
     */
    public ChronicleQueueBuilder bindAddress(InetSocketAddress bindAddress) {
        this.replicaConfig.bindAddress = bindAddress;
        return this;
    }

    /**
     *
     * @param connectAddress
     *
     * @return this builder object back
     */
    public ChronicleQueueBuilder connectAddress(InetSocketAddress connectAddress) {
        this.replicaConfig.connectAddress = connectAddress;
        return this;
    }

    // *************************************************************************
    //
    // *************************************************************************

    public static ChronicleQueueBuilder of(ChronicleQueueType type, File path) {
        return new ChronicleQueueBuilder(type, path);
    }

    public static ChronicleQueueBuilder of(ChronicleQueueType type, String path) {
        return of(type, new File(path));
    }

    public static ChronicleQueueBuilder of(ChronicleQueueType type, String parent, String child) {
        return of(type, new File(parent, child));
    }

    public static ChronicleQueueBuilder of(ChronicleQueueType type, File parent, String child) {
        return of(type, new File(parent, child));
    }

    // *************************************************************************
    //
    // *************************************************************************

    private class IndexedConfig {
        private int indexFileCapacity = 16 * 1024;
        private int indexFileExcerpts  = 16 * 1024 * 1024;
        private int cacheLineSize = 64;
        private int dataBlockSize = 512 * 1024 * 1024;
        private int messageCapacity = 128 * 1024;
        private int indexBlockSize = Math.max(4096, this.dataBlockSize / 4);;
        private boolean minimiseFootprint = false;
        private ByteOrder byteOrder = ByteOrder.nativeOrder();
    }

    private class VanillaConfig {
        private String cycleFormat = "yyyyMMdd";
        private int cycleLength = 24 * 60 * 60 * 1000; // MILLIS_PER_DAY
        private int defaultMessageSize = 128 << 10; // 128 KB.
        private int dataCacheCapacity = 32;
        private int indexCacheCapacity = 32;
        private long indexBlockSize = 16L << 20; // 16 MB
        private long dataBlockSize = 64L << 20; // 64 MB
        private long entriesPerCycle = 1L << 40; // one trillion per day or per hour.
        private boolean cleanupOnClose = false;
    }

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
}
