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
 * This listens to a ChronicleSource and copies new entries.
 * This Sink can be any number of excerpt behind the source
 * and can be restart many times without losing data.
 *
 * <p>Can be used as a component with lower over head than ChronicleSink
 *
 * @author peter.lawrey
 */
public class VanillaChronicleSink implements Chronicle {
    @NotNull
    private final VanillaChronicle chronicle;
    @NotNull
    private final SocketAddress address;
    @Nullable
    private SocketChannel sc = null;

    private final VanillaChronicle.VanillaAppender excerpt;
    private final Logger logger;
    private volatile boolean closed = false;

    public VanillaChronicleSink(@NotNull VanillaChronicle chronicle, String hostname, int port) throws IOException {
        this.chronicle = chronicle;
        this.address = new InetSocketAddress(hostname, port);
        logger = LoggerFactory.getLogger(getClass().getName() + '.' + chronicle);
        excerpt = chronicle.createAppender();
        readBuffer = TcpUtil.createBuffer(256 * 1024, ByteOrder.nativeOrder());
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

    private class SinkExcerpt extends WrappedExcerpt {
        @SuppressWarnings("unchecked")
        public SinkExcerpt(ExcerptCommon excerpt) {
            super(excerpt);
        }

        @Override
        public boolean nextIndex() {
            if(super.nextIndex()) {
                return true;
            }

            return readNext() && super.nextIndex();
        }

        @Override
        public boolean index(long index) throws IndexOutOfBoundsException {
            if (super.index(index)) {
                return true;
            }

            return readNext() && super.index(index);
        }
    }

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
                readBuffer.clear();
                readBuffer.limit(0);

                SocketChannel sc = SocketChannel.open(address);
                sc.socket().setReceiveBufferSize(256 * 1024);
                logger.info("Connected to " + address);
                ByteBuffer bb = ByteBuffer.allocate(8);
                bb.putLong(0, chronicle.lastIndex());
                TcpUtil.writeAllOrEOF(sc, bb);
                return sc;

            } catch (IOException e) {
                logger.info("Failed to connect to {} retrying ", address, e);
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

    private final ByteBuffer readBuffer; // minimum size

    private boolean readNextExcerpt(@NotNull SocketChannel sc) {
        try {
            if (closed) {
                return false;
            }

            // Check if there is enogh data (header plus some more data)
            if (readBuffer.remaining() < TcpUtil.HEADER_SIZE) {
                if (readBuffer.remaining() == 0){
                    readBuffer.clear();
                } else {
                    readBuffer.compact();
                }

                // Waith till some more data has been readed
                while (readBuffer.position() < TcpUtil.HEADER_SIZE + 8) {
                    if (sc.read(readBuffer) < 0) {
                        sc.close();
                        return false;
                    }
                }

                readBuffer.flip();
            }

            int size = readBuffer.getInt();
            long scIndex = readBuffer.getLong();

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

            int cycle = (int) (scIndex >>> chronicle.getEntriesForCycleBits());

            excerpt.startExcerpt(size,cycle);
            // perform a progressive copy of data.
            long remaining = size;
            int limit = readBuffer.limit();
            int size2 = (int) Math.min(readBuffer.remaining(), remaining);
            remaining -= size2;
            readBuffer.limit(readBuffer.position() + size2);
            excerpt.write(readBuffer);
            // reset the limit;
            readBuffer.limit(limit);

            // needs more than one read.
            while (remaining > 0) {
                readBuffer.clear();
                int size3 = (int) Math.min(readBuffer.capacity(), remaining);
                readBuffer.limit(size3);
                if (sc.read(readBuffer) < 0)
                    throw new EOFException();
                readBuffer.flip();
                remaining -= readBuffer.remaining();
                excerpt.write(readBuffer);
            }

            excerpt.finish();
        } catch (IOException e) {
            logger.info("Lost connection to {} retrying ",address, e);
            try {
                sc.close();
            } catch (IOException ignored) {
            }
        }

        return true;
    }

    void closeSocket(@Nullable SocketChannel sc) {
        if (sc != null)
            try {
                sc.close();
            } catch (IOException e) {
                logger.warn("Error closing socket", e);
            }
    }

    @Override
    public void close() {
        closed = true;
        closeSocket(sc);
        excerpt.close();
        chronicle.close();
    }

    public ChronicleConfig config() {
        throw new UnsupportedOperationException();
    }

    public void clear() {
        chronicle.clear();
    }

    public void checkCounts(int min, int max) {
        chronicle.checkCounts(min, max);
    }
}
