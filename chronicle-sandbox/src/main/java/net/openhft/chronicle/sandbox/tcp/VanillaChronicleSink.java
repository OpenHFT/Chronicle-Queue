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

package net.openhft.chronicle.sandbox.tcp;

import net.openhft.chronicle.*;
import net.openhft.chronicle.tcp.TcpUtil;
import net.openhft.chronicle.tools.WrappedExcerpt;
import net.openhft.lang.model.constraints.NotNull;
import net.openhft.lang.model.constraints.Nullable;

import java.io.EOFException;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

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
    private final VanillaChronicle.VanillaAppender excerpt;
    private final Logger logger;
    private volatile boolean closed = false;

    public VanillaChronicleSink(@NotNull VanillaChronicle chronicle, String hostname, int port) throws IOException {
        this.chronicle = chronicle;
        this.address = new InetSocketAddress(hostname, port);
        logger = Logger.getLogger(getClass().getName() + '.' + chronicle);
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
            return super.nextIndex() || readNext() && super.nextIndex();
        }

        @Override
        public boolean index(long index) throws IndexOutOfBoundsException {
            if (super.nextIndex()) return true;
            if (readNext()) {
                return super.nextIndex();
            }
            return nextIndex();
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
                if (logger.isLoggable(Level.FINE))
                    logger.log(Level.FINE, "Failed to connect to " + address + " retrying", e);
                else if (logger.isLoggable(Level.INFO))
                    logger.log(Level.INFO, "Failed to connect to " + address + " retrying " + e);
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
            if (closed) return false;

            if (readBuffer.remaining() < TcpUtil.HEADER_SIZE +8) {
                if (readBuffer.remaining() == 0){
                    readBuffer.clear();
                }
                else{
                    readBuffer.compact();
                }
                while (readBuffer.position() < 8 + 4 + 8 + 8) {
                    if (sc.read(readBuffer) < 0) {
                        sc.close();
                        return false;
                    }
                }
                readBuffer.flip();
            }

            long scIndex = readBuffer.getLong();
            int size = readBuffer.getInt();

            if(size == VanillaChronicleSource.IN_SYNC_LEN){
                //Heartbeat message ignore and return false
                return false;
            }

            if (size > 128 << 20 || size < 0)
                throw new StreamCorruptedException("size was " + size);

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
            if (logger.isLoggable(Level.FINE))
                logger.log(Level.FINE, "Lost connection to " + address + " retrying", e);
            else if (logger.isLoggable(Level.INFO))
                logger.log(Level.INFO, "Lost connection to " + address + " retrying " + e);
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
                logger.warning("Error closing socket " + e);
            }
    }

    @Override
    public void close() {
        closed = true;
        closeSocket(sc);
        chronicle.close();
    }

    public ChronicleConfig config() {
        throw new UnsupportedOperationException();
    }

    public void clear() {
        chronicle.clear();
    }
}
