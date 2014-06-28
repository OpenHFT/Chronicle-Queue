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
import net.openhft.chronicle.ChronicleConfig;
import net.openhft.chronicle.Excerpt;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptCommon;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.IndexedChronicle;
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
 * This listens to a ChronicleSource and copies new entries. This SInk can be any number of excerpt behind the source
 * and can be restart many times without losing data.
 *
 * <p>Can be used as a component with lower over head than ChronicleSink
 *
 * @author peter.lawrey
 */
public class InProcessChronicleSink implements Chronicle {
    @NotNull
    private final Chronicle chronicle;
    @NotNull
    private final SocketAddress address;
    private final ExcerptAppender excerpt;
    private final Logger logger;
    private final ByteBuffer buffer; // minimum size
    private volatile boolean closed = false;

    public InProcessChronicleSink(@NotNull Chronicle chronicle, String hostname, int port) throws IOException {
        this.chronicle = chronicle;
        this.address = new InetSocketAddress(hostname, port);
        this.logger = LoggerFactory.getLogger(getClass().getName() + '.' + hostname + '@' + port);
        this.excerpt = chronicle.createAppender();
        this.buffer = TcpUtil.createBuffer(256 * 1024, ByteOrder.nativeOrder());
    }

    @Override
    public String name() {
        return chronicle.name();
    }

    @NotNull
    @Override
    public Excerpt createExcerpt() throws IOException {
        return new SinkExcerpt(chronicle.createExcerpt());
    }

    @NotNull
    @Override
    public ExcerptTailer createTailer() throws IOException {
        return new SinkExcerpt(chronicle.createTailer());
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

    private class SinkExcerpt extends WrappedExcerpt {
        @SuppressWarnings("unchecked")
        public SinkExcerpt(ExcerptCommon excerpt) {
            super(excerpt);
        }

        @Override
        public boolean nextIndex() {
            return super.nextIndex() || readNext() && super.nextIndex();
        }

        @Override
        public boolean index(long index) throws IndexOutOfBoundsException {
            return super.index(index) || index >= 0 && readNext() && super.index(index);
        }
    }

    @Nullable
    private SocketChannel sc = null;

    boolean readNext() {
        if (sc == null || !sc.isOpen()) {
            sc = createConnection();
        }
        return sc != null && readNextExcerpt(sc);
    }

    @Nullable
    private SocketChannel createConnection() {
        while (!closed) {
            try {
                buffer.clear();
                buffer.limit(0);

                SocketChannel sc = SocketChannel.open(address);
                sc.socket().setReceiveBufferSize(256 * 1024);
                logger.info("Connected to " + address);

                ByteBuffer bb = ByteBuffer.allocate(8);
                bb.putLong(0, chronicle.lastWrittenIndex());
                TcpUtil.writeAllOrEOF(sc, bb);

                return sc;
            } catch (IOException e) {
                logger.info("Failed to connect to {} retrying", address, e);
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
        return null;
    }



    private boolean readNextExcerpt(@NotNull SocketChannel sc) {
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
                    excerpt.startExcerpt(((IndexedChronicle) chronicle).config().dataBlockSize() - 1);
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

            excerpt.startExcerpt(size);
            // perform a progressive copy of data.
            long remaining = size;
            int limit = buffer.limit();

            int size2 = (int) Math.min(buffer.remaining(), remaining);
            remaining -= size2;
            buffer.limit(buffer.position() + size2);
            excerpt.write(buffer);
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
                excerpt.write(buffer);
            }

            excerpt.finish();
        } catch (IOException e) {
            logger.info("Lost connection to {} retrying", address, e);
            try {
                sc.close();
            } catch (IOException ignored) {
            }
        }
        return true;
    }

    void closeSocket(@Nullable SocketChannel sc) {
        if (sc != null) {
            try {
                sc.close();
            } catch (IOException e) {
                logger.warn("Error closing socket", e);
            }
        }
    }

    @Override
    public void close() {
        closed = true;
        closeSocket(sc);

        try {
            chronicle.close();
        } catch (IOException e) {
            logger.warn("Error closing Sink", e);
        }
    }

    public ChronicleConfig config() {
        return ((IndexedChronicle) chronicle).config();
    }
}
