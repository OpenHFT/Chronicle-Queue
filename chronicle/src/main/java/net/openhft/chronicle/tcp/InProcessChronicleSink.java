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

package net.openhft.chronicle.tcp;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.Excerpt;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptCommon;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.IndexedChronicle;
import net.openhft.chronicle.VanillaChronicle;
import net.openhft.chronicle.tools.WrappedExcerpt;
import net.openhft.lang.model.constraints.NotNull;
import net.openhft.lang.model.constraints.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;

/**
 * This listens to a ChronicleSource and copies new entries. This Sink can be any number of excerpt behind the source
 * and can be restart many times without losing data.
 *
 * <p>Can be used as a component with lower over head than ChronicleSink
 *
 * @author peter.lawrey
 */
public class InProcessChronicleSink extends ChronicleSink {
    @NotNull
    private final Chronicle chronicle;

    @NotNull
    private final ChronicleSinkConfig sinkConfig;

    @NotNull
    private final SocketAddress address;

    @Nullable
    private WrappedExcerpt liveExcerpt;

    private final Logger logger;
    private volatile boolean closed = false;

    public InProcessChronicleSink(@NotNull Chronicle chronicle, String hostname, int port) throws IOException {
        this(chronicle, ChronicleSinkConfig.DEFAULT, new InetSocketAddress(hostname, port));
    }

    public InProcessChronicleSink(@NotNull Chronicle chronicle, @NotNull final ChronicleSinkConfig config, String hostname, int port) throws IOException {
        this(chronicle, config, new InetSocketAddress(hostname, port));
    }

    public InProcessChronicleSink(@NotNull final Chronicle chronicle, @NotNull final InetSocketAddress address) throws IOException {
        this(chronicle, ChronicleSinkConfig.DEFAULT, address);
    }

    public InProcessChronicleSink(@NotNull final Chronicle chronicle, @NotNull final ChronicleSinkConfig sinkConfig, @NotNull final InetSocketAddress address) throws IOException {
        this.chronicle = chronicle;
        this.sinkConfig = sinkConfig;
        this.address = address;
        this.logger = LoggerFactory.getLogger(getClass().getName() + '.' + address.getHostName() + '@' + address.getPort());
        this.liveExcerpt = null;
    }

    @Override
    public String name() {
        return chronicle.name();
    }

    @NotNull
    @Override
    public synchronized Excerpt createExcerpt() throws IOException {
        if(liveExcerpt != null) {
            throw new UnsupportedOperationException("An Excerpt has already been created");
        }

        return liveExcerpt = (chronicle instanceof IndexedChronicle)
            ? new IndexedSinkExcerpt(chronicle.createExcerpt())
            : new VanillaSinkExcerpt(chronicle.createExcerpt());
    }

    @NotNull
    @Override
    public synchronized ExcerptTailer createTailer() throws IOException {
        if(liveExcerpt != null) {
            throw new UnsupportedOperationException("An Excerpt has already been created");
        }

        return liveExcerpt = (chronicle instanceof IndexedChronicle)
            ? new IndexedSinkExcerpt(chronicle.createTailer())
            : new VanillaSinkExcerpt(chronicle.createExcerpt());
    }

    @NotNull
    @Override
    public ExcerptAppender createAppender() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long lastWrittenIndex() {
        return chronicle.lastWrittenIndex();
    }

    @Override
    public long size() {
        return chronicle.size();
    }

    @Override
    public void clear() {
        chronicle.clear();
    }

    @Override
    public void close() {
        closed = true;

        if(liveExcerpt != null) {
            liveExcerpt.close();
        }

        try {
            chronicle.close();
        } catch (IOException e) {
            logger.warn("Error closing Sink", e);
        }
    }

    public void checkCounts(int min, int max) {
        if(chronicle instanceof VanillaChronicle) {
            ((VanillaChronicle)chronicle).checkCounts(min, max);
        }
    }

    // *************************************************************************
    //
    // *************************************************************************

    private abstract class AbstractSinkExcerpt<T extends Chronicle>  extends WrappedExcerpt {
        protected final T chronicleImpl;
        protected final ByteBuffer buffer;
        private SocketChannel socketChannel;
        protected long lastLocalIndex;

        public AbstractSinkExcerpt(ExcerptCommon excerptCommon) {
            super(excerptCommon);
            this.chronicleImpl = (T)chronicle;
            this.socketChannel = null;
            this.buffer = TcpUtil.createBuffer(sinkConfig.minBufferSize(), ByteOrder.nativeOrder());
            this.lastLocalIndex = -1;
        }

        @Override
        public boolean nextIndex() {
            return super.nextIndex() || (readNext() && super.nextIndex());
        }

        @Override
        public boolean index(long index) throws IndexOutOfBoundsException {
            return super.index(index) || (index >= 0 && readNext() && super.index(index));
        }

        @Override
        public synchronized void close() {
            if(socketChannel != null) {
                try {
                    socketChannel.close();
                } catch (IOException e) {
                    logger.warn("Error closing socket", e);
                }
            }

            liveExcerpt = null;

            super.close();
        }

        private boolean readNext() {
            if (socketChannel == null || !socketChannel.isOpen()) {
                socketChannel = createConnection();
            }

            return socketChannel != null && readNextExcerpt(socketChannel);
        }

        private SocketChannel createConnection() {
            while (!closed) {
                try {
                    buffer.clear();
                    buffer.limit(0);

                    SocketChannel sc = SocketChannel.open(address);
                    sc.socket().setReceiveBufferSize(sinkConfig.minBufferSize());
                    logger.info("Connected to " + address);

                    lastLocalIndex = lastIndex();

                    ByteBuffer bb = ByteBuffer.allocate(8);
                    bb.putLong(0, lastLocalIndex);
                    TcpUtil.writeAllOrEOF(sc, bb);

                    return sc;
                } catch (IOException e) {
                    logger.info("Failed to connect to {} retrying", address, e);
                }

                try {
                    Thread.sleep(sinkConfig.reconnectDelay());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
            return null;
        }

        protected abstract long lastIndex();
        protected abstract boolean readNextExcerpt(final SocketChannel channel);
    }

    /**
     * IndexedChronicle sink
     */
    private final class IndexedSinkExcerpt extends AbstractSinkExcerpt<IndexedChronicle> {
        @NotNull
        private final ExcerptAppender appender;

        @SuppressWarnings("unchecked")
        public IndexedSinkExcerpt(@NotNull final ExcerptCommon excerpt) throws IOException {
            super(excerpt);
            this.appender = chronicle.createAppender();
        }

        @Override
        protected long lastIndex()  {
            return chronicle.lastWrittenIndex();
        }

        @Override
        protected boolean readNextExcerpt(@NotNull final SocketChannel sc) {
            try {
                if (closed) return false;

                if (buffer.remaining() < TcpUtil.HEADER_SIZE) {
                    if (buffer.remaining() == 0) {
                        buffer.clear();
                    } else {
                        buffer.compact();
                    }

                    int minSize = TcpUtil.HEADER_SIZE + 8;
                    while (buffer.position() < minSize) {
                        if (sc.read(buffer) < 0) {
                            sc.close();
                            return false;
                        }
                    }
                    buffer.flip();
                }

                int size = buffer.getInt();
                switch (size) {
                    case ChronicleSource.IN_SYNC_LEN:
                        buffer.getLong();
                        return false;
                    case ChronicleSource.PADDED_LEN:
                        appender.startExcerpt(((IndexedChronicle) chronicle).config().dataBlockSize() - 1);
                        buffer.getLong();
                        return true;
                    case ChronicleSource.SYNC_IDX_LEN:
                        buffer.getLong();
                        //Sync IDX message, re-try
                        return readNextExcerpt(sc);
                    default:
                        break;
                }

                if (size > 128 << 20 || size < 0)
                    throw new StreamCorruptedException("size was " + size);

                final long scIndex = buffer.getLong();
                if (scIndex != chronicle.size()) {
                    throw new StreamCorruptedException("Expected index " + chronicle.size() + " but got " + scIndex);
                }

                appender.startExcerpt(size);
                // perform a progressive copy of data.
                long remaining = size;
                int limit = buffer.limit();

                int size2 = (int) Math.min(buffer.remaining(), remaining);
                remaining -= size2;
                buffer.limit(buffer.position() + size2);
                appender.write(buffer);
                // reset the limit;
                buffer.limit(limit);

                // needs more than one read.
                while (remaining > 0) {
                    buffer.clear();
                    int size3 = (int) Math.min(buffer.capacity(), remaining);
                    buffer.limit(size3);
                    if (sc.read(buffer) < 0)
                        throw new EOFException();
                    buffer.flip();
                    remaining -= buffer.remaining();
                    appender.write(buffer);
                }

                appender.finish();
            } catch (IOException e) {
                logger.info("Lost connection to {} retrying", address, e);
                try {
                    sc.close();
                } catch (IOException ignored) {
                }
            }
            return true;
        }
    }

    /**
     * VanillaChronicle sink
     */
    private final class VanillaSinkExcerpt extends AbstractSinkExcerpt<VanillaChronicle> {
        @NotNull
        private final VanillaChronicle.VanillaAppender appender;

        @SuppressWarnings("unchecked")
        public VanillaSinkExcerpt(@NotNull final ExcerptCommon excerpt) throws IOException {
            super(excerpt);
            this.appender = chronicleImpl.createAppender();
        }

        @Override
        protected long lastIndex()  {
            return chronicleImpl.lastIndex();
        }

        @Override
        protected boolean readNextExcerpt(@NotNull final SocketChannel sc) {
            try {
                if (closed) {
                    return false;
                }

                // Check if there is enogh data (header plus some more data)
                if (buffer.remaining() < TcpUtil.HEADER_SIZE) {
                    if (buffer.remaining() == 0){
                        buffer.clear();
                    } else {
                        buffer.compact();
                    }

                    // Waith till some more data has been readed
                    while (buffer.position() < TcpUtil.HEADER_SIZE + 8) {
                        if (sc.read(buffer) < 0) {
                            sc.close();
                            return false;
                        }
                    }

                    buffer.flip();
                }

                int size = buffer.getInt();
                long scIndex = buffer.getLong();

                switch (size) {
                    case ChronicleSource.IN_SYNC_LEN:
                        //Heartbeat message ignore and return false
                        return false;
                    case ChronicleSource.PADDED_LEN:
                        //Padded message, should not happen
                        return false;
                    case ChronicleSource.SYNC_IDX_LEN:
                        //Sync IDX message, re-try
                        return readNextExcerpt(sc);
                    default:
                        break;
                }

                if (size > 128 << 20 || size < 0) {
                    throw new StreamCorruptedException("size was " + size);
                }

                if(lastLocalIndex != scIndex) {

                    int cycle = (int) (scIndex >>> chronicleImpl.getEntriesForCycleBits());

                    appender.startExcerpt(size, cycle);
                    // perform a progressive copy of data.
                    long remaining = size;
                    int limit = buffer.limit();
                    int size2 = (int) Math.min(buffer.remaining(), remaining);
                    remaining -= size2;
                    buffer.limit(buffer.position() + size2);
                    appender.write(buffer);
                    // reset the limit;
                    buffer.limit(limit);

                    // needs more than one read.
                    while (remaining > 0) {
                        buffer.clear();
                        int size3 = (int) Math.min(buffer.capacity(), remaining);
                        buffer.limit(size3);
                        if (sc.read(buffer) < 0)
                            throw new EOFException();
                        buffer.flip();
                        remaining -= buffer.remaining();
                        appender.write(buffer);
                    }

                    appender.finish();
                } else {
                    buffer.position(buffer.position() + size);
                    return readNextExcerpt(sc);
                }
            } catch (IOException e) {
                logger.info("Lost connection to {} retrying ",address, e);
                try {
                    sc.close();
                } catch (IOException ignored) {
                }
            }

            return true;
        }
    }
}
