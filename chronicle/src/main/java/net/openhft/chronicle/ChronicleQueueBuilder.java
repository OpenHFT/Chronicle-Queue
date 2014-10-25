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


import net.openhft.chronicle.tcp.ChronicleSinkConfig;
import net.openhft.chronicle.tcp2.*;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;

public class ChronicleQueueBuilder {

    private final File path;
    private final ChronicleQueueType type;
    private final IndexedConfig indexedConfig;
    private final VanillaConfig vanillaConfig;
    private final ReplicaConfig replicaConfig;

    private final ReplicaType replicaType;

    private boolean synchonous;
    private boolean useCheckedExcerpt;

    /**
     * @param type
     * @param path
     */
    private ChronicleQueueBuilder(ChronicleQueueType type, File path) {
        this.type = type;
        this.path = path;
        this.replicaType = null;
        this.synchonous = false;
        this.useCheckedExcerpt = false;
        this.indexedConfig = new IndexedConfig();
        this.vanillaConfig = new VanillaConfig();
        this.replicaConfig = new ReplicaConfig();
    }

    /**
     * @param type
     * @param path
     * @param replicaType
     */
    private ChronicleQueueBuilder(ChronicleQueueType type, File path, ReplicaType replicaType) {
        this.type = type;
        this.path = path;
        this.replicaType = replicaType;
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

    public Chronicle build() throws IOException {
        if(replicaType == null) {
            if(replicaType == ReplicaType.SINK) {
                return buildSinkChronicle();
            } if(replicaType == ReplicaType.SOURCE) {
                return buildSourceChronicle();
            }
        } else {
            if(type == ChronicleQueueType.INDEXED) {
                return buildIndexedChronicle();
            } if(type == ChronicleQueueType.VANILLA) {
                return buildVanillaChronicle();
            }

        }

        return null;
    }

    private Chronicle buildIndexedChronicle() throws IOException {
        Chronicle chronicle = new IndexedChronicle(this.path.getAbsolutePath());
        return chronicle;
    }

    private Chronicle buildVanillaChronicle() throws IOException {
        Chronicle chronicle = new VanillaChronicle(this.path.getAbsolutePath());
        return chronicle;
    }

    private Chronicle buildSinkChronicle() throws IOException {
        SinkTcpConnection cnx = null;
        Chronicle chron = null;

        if(this.replicaConfig.bindAddress != null && this.replicaConfig.connectAddress != null) {
            // INITIATOR
            cnx = new SinkTcpConnectionInitiator(replicaConfig.connectAddress, replicaConfig.bindAddress);
            cnx.receiveBufferSize(replicaConfig.receiveBufferSize);
            cnx.reconnectTimeout(replicaConfig.reconnectTimeout, replicaConfig.reconnectTimeoutUnit);
            cnx.selectTimeout(replicaConfig.selectTimeout, replicaConfig.selectTimeoutUnit);
            cnx.maxOpenAttempts(replicaConfig.maxOpenAttempts);
        } else if(this.replicaConfig.connectAddress != null){
            // INITIATOR
            cnx = new SinkTcpConnectionAcceptor(replicaConfig.bindAddress);
            cnx.receiveBufferSize(replicaConfig.receiveBufferSize);
            cnx.reconnectTimeout(replicaConfig.reconnectTimeout, replicaConfig.reconnectTimeoutUnit);
            cnx.selectTimeout(replicaConfig.selectTimeout, replicaConfig.selectTimeoutUnit);
            cnx.maxOpenAttempts(replicaConfig.maxOpenAttempts);
        } else if(this.replicaConfig.bindAddress != null){
            // ACCEPTOR
            cnx = new SinkTcpConnectionAcceptor(replicaConfig.bindAddress);
            cnx.receiveBufferSize(replicaConfig.receiveBufferSize);
            cnx.reconnectTimeout(replicaConfig.reconnectTimeout, replicaConfig.reconnectTimeoutUnit);
            cnx.selectTimeout(replicaConfig.selectTimeout, replicaConfig.selectTimeoutUnit);
            cnx.maxOpenAttempts(replicaConfig.maxOpenAttempts);
        }

        if(type != null) {
            if (type == ChronicleQueueType.INDEXED) {
                chron = buildIndexedChronicle();
            } else if (type == ChronicleQueueType.VANILLA) {
                chron = buildVanillaChronicle();
            }
        }

        return new ChronicleSink2(chron, ChronicleSinkConfig.DEFAULT.clone() , cnx);
    }

    private Chronicle buildSourceChronicle() throws IOException {
        return null;
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


    public static ChronicleQueueBuilder sink(ChronicleQueueType type, File path, InetSocketAddress bindAddress, InetSocketAddress connectAddress) {
        return new ChronicleQueueBuilder(type, path, ReplicaType.SINK)
            .bindAddress(bindAddress)
            .connectAddress(connectAddress);
    }

    public static ChronicleQueueBuilder sink(ChronicleQueueType type, String path, InetSocketAddress bindAddress, InetSocketAddress connectAddress) {
        return sink(type, new File(path), bindAddress, connectAddress);
    }

    public static ChronicleQueueBuilder sink(ChronicleQueueType type, String parent, String child, InetSocketAddress bindAddress, InetSocketAddress connectAddress) {
        return sink(type, new File(parent, child), bindAddress, connectAddress);
    }

    public static ChronicleQueueBuilder sink(ChronicleQueueType type, File parent, String child, InetSocketAddress bindAddress, InetSocketAddress connectAddress) {
        return sink(type, new File(parent, child), bindAddress, connectAddress);
    }

    public static ChronicleQueueBuilder sink(InetSocketAddress bindAddress, InetSocketAddress connectAddress) {
        return sink(null, (File) null, bindAddress, connectAddress);
    }


    public static ChronicleQueueBuilder source(ChronicleQueueType type, File path, InetSocketAddress bindAddress, InetSocketAddress connectAddress) {
        return new ChronicleQueueBuilder(type, path, ReplicaType.SOURCE)
            .bindAddress(bindAddress)
            .connectAddress(connectAddress);
    }

    public static ChronicleQueueBuilder source(ChronicleQueueType type, String path, InetSocketAddress bindAddress, InetSocketAddress connectAddress) {
        return source(type, new File(path), bindAddress, connectAddress);
    }

    public static ChronicleQueueBuilder source(ChronicleQueueType type, String parent, String child, InetSocketAddress bindAddress, InetSocketAddress connectAddress) {
        return source(type, new File(parent, child), bindAddress, connectAddress);
    }

    public static ChronicleQueueBuilder source(ChronicleQueueType type, File parent, String child, InetSocketAddress bindAddress, InetSocketAddress connectAddress) {
        return source(type, new File(parent, child), bindAddress, connectAddress);
    }

    public static ChronicleQueueBuilder source(InetSocketAddress bindAddress, InetSocketAddress connectAddress) {
        return source(null, (File) null, bindAddress, connectAddress);
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

    private enum ReplicaType {
        SINK,
        SOURCE
    }
}
