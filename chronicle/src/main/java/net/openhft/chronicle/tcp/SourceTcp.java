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
package net.openhft.chronicle.tcp;

import net.openhft.chronicle.*;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.model.constraints.NotNull;
import net.openhft.lang.thread.LightPauser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class SourceTcp {
    protected final Logger logger;
    protected final String name;
    protected final AtomicBoolean running;
    protected final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder;
    protected final ThreadPoolExecutor executor;
    protected final LightPauser pauser = new LightPauser(10000, 10000);

    protected SourceTcp(String name, final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder, ThreadPoolExecutor executor) {
        this.builder = builder;
        this.name = ChronicleTcp.connectionName(name, this.builder.bindAddress(), this.builder.connectAddress());
        this.logger = LoggerFactory.getLogger(this.name);
        this.running = new AtomicBoolean(false);
        this.executor = executor;
    }

    public boolean open() {
        this.running.set(true);
        this.executor.execute(createHandler());

        return this.running.get();
    }

    public boolean close() {
        running.set(false);
        executor.shutdown();

        try {
            executor.awaitTermination(
                    builder.selectTimeout() * 2,
                    builder.selectTimeoutUnit());
        } catch (InterruptedException e) {
            // Ignored
        }

        return !running.get();
    }

    @Override
    public String toString() {
        return this.name;
    }

    public abstract boolean isLocalhost();

    protected abstract Runnable createHandler();


    /**
     * Creates a session handler according to the Chronicle the sources is connected to.
     *
     * @param socketChannel The {@link java.nio.channels.SocketChannel}
     * @return The Runnable
     */
    protected Runnable createSessionHandler(final @NotNull SocketChannel socketChannel) {
        final Chronicle chronicle = builder.chronicle();
        if (chronicle != null) {
            if (chronicle instanceof IndexedChronicle) {
                return new IndexedSessionHandler(socketChannel);
            } else if (chronicle instanceof VanillaChronicle) {
                return new VanillaSessionHandler(socketChannel);
            } else {
                throw new IllegalStateException("Chronicle must be Indexed or Vanilla");
            }
        }

        throw new IllegalStateException("Chronicle can't be null");
    }

    // *************************************************************************
    //
    // *************************************************************************

    /**
     * Abstract class for Indexed and Vanilla chronicle replicaton
     */
    private abstract class SessionHandler implements Runnable, Closeable {
        private final SocketChannel socketChannel;

        private long lastUnpausedNS;

        protected final TcpConnection connection;
        protected ExcerptTailer tailer;
        protected long lastHeartbeat;

        protected final ByteBuffer writeBuffer;


        // this could be re-sized so cannot be final
        protected ByteBufferBytes readBuffer;

        private SessionHandler(final @NotNull SocketChannel socketChannel) {
            this.socketChannel = socketChannel;
            this.connection = new TcpConnection(socketChannel);
            this.tailer = null;
            this.lastHeartbeat = 0;
            this.lastUnpausedNS = 0;
            this.readBuffer = new ByteBufferBytes(ChronicleTcp.createBufferOfSize(16));
            this.writeBuffer = ChronicleTcp.createBuffer(builder.minBufferSize());

            this.readBuffer.clear();

            this.writeBuffer.clear();
            this.writeBuffer.limit(0);
        }


        @Override
        public void close() throws IOException {
            if (this.tailer != null) {
                this.tailer.close();
                this.tailer = null;
            }

            if (this.socketChannel.isOpen()) {
                this.socketChannel.close();
            }
        }

        @Override
        public void run() {
            VanillaSelectionKeySet selectionKeys = null;

            try {
                socketChannel.configureBlocking(false);
                socketChannel.socket().setSendBufferSize(builder.minBufferSize());
                socketChannel.socket().setTcpNoDelay(true);
                socketChannel.socket().setSoTimeout(0);
                socketChannel.socket().setSoLinger(false, 0);

                final VanillaSelector selector = new VanillaSelector()
                        .open()
                        .register(socketChannel, SelectionKey.OP_READ, new Attached());

                tailer = builder.chronicle().createTailer();
                selectionKeys = selector.vanillaSelectionKeys();

                if (selectionKeys != null) {
                    vanillaNioLoop(selector, selectionKeys);
                } else {
                    nioLoop(selector);
                }
            } catch (EOFException e) {
                if (running.get()) {
                    logger.info("Connection {} died", socketChannel);
                }
            } catch (Exception e) {
                if (running.get()) {
                    String msg = e.getMessage();
                    if (msg != null &&
                            (msg.contains("reset by peer")
                                    || msg.contains("Broken pipe")
                                    || msg.contains("was aborted by"))) {
                        logger.info("Connection {} closed from the other end: ", socketChannel, e.getMessage());
                    } else {
                        logger.info("Connection {} died", socketChannel, e);
                    }
                }
            } finally {
                if (selectionKeys != null) {
                    selectionKeys.clear();
                }
            }

            try {
                close();
            } catch (IOException e) {
                logger.warn("", e);
            }
        }

        private void vanillaNioLoop(final VanillaSelector selector, final VanillaSelectionKeySet selectionKeys) throws IOException {
            final int spinLoopCount = builder.selectorSpinLoopCount();
            final long selectTimeout = builder.selectTimeout();

            while (running.get()) {
                int nbKeys = selector.select(spinLoopCount, selectTimeout);

                if (nbKeys > 0) {
                    final SelectionKey[] keys = selectionKeys.keys();
                    final int size = selectionKeys.size();

                    for (int k = 0; k < size; k++) {
                        final SelectionKey key = keys[k];
                        if (key != null) {
                            if (!onSelectionKey(key)) {
                                break;
                            }
                        }
                    }

                    selectionKeys.clear();
                }
            }
        }

        private void nioLoop(final VanillaSelector selector) throws IOException {
            final int spinLoopCount = builder.selectorSpinLoopCount();
            final long selectTimeout = builder.selectTimeout();

            while (running.get()) {
                int nbKeys = selector.select(spinLoopCount, selectTimeout);

                if (nbKeys > 0) {
                    final Set<SelectionKey> keys = selector.selectionKeys();
                    for (final SelectionKey key : keys) {
                        if (!onSelectionKey(key)) {
                            break;
                        }
                    }

                    keys.clear();
                }
            }
        }

        protected boolean hasRoomForExcerpt(ByteBuffer buffer, Bytes tailer) {
            return (tailer.capacity() + ChronicleTcp.HEADER_SIZE) < (buffer.capacity() - buffer.position());
        }

        protected void pauseReset() {
            lastUnpausedNS = System.nanoTime();
            pauser.reset();
        }

        protected void pause() {
            if (lastUnpausedNS + ChronicleTcp.BUSY_WAIT_TIME_NS > System.nanoTime()) {
                return;
            }

            pauser.pause();
        }

        protected void setLastHeartbeat() {
            this.lastHeartbeat = System.currentTimeMillis() + builder.heartbeatIntervalMillis();
        }

        protected void setLastHeartbeat(long from) {
            this.lastHeartbeat = from + builder.heartbeatIntervalMillis();
        }

        protected void sendSizeAndIndex(int size, long index) throws IOException {
            writeBuffer.clear();
            writeBuffer.putInt(size);
            writeBuffer.putLong(index);
            writeBuffer.flip();
            connection.writeAllOrEOF(writeBuffer);
            setLastHeartbeat();
        }

        protected boolean onSelectionKey(final SelectionKey key) throws IOException {
            if (key != null) {
                if (key.isReadable()) {
                    if (!onRead(key)) {
                        return false;
                    }
                } else if (key.isWritable()) {
                    if (!onWrite(key)) {
                        return false;
                    }
                }
            }

            return true;
        }

        protected boolean onRead(final SelectionKey key) throws IOException {
            try {

                final long action = readUpTo(8).readLong();

                switch ((int) action) {

                    case (int) ChronicleTcp.ACTION_WITH_MAPPING:
                        int size = readUpTo(4).readInt();

                        Object attachment = key.attachment();
                        if (attachment != null) {
                            MappingFunction mappingFunction = readUpTo(size).readObject(MappingFunction.class);
                            ((MappingProvider) attachment).withMapping(mappingFunction);
                        }

                        return true;

                    case (int) ChronicleTcp.ACTION_SUBSCRIBE:
                        return onSubscribe(key, readUpTo(8).readLong());

                    case (int) ChronicleTcp.ACTION_QUERY:
                        return onQuery(key, readUpTo(8).readLong());

                    default:
                        throw new IOException("Unknown action received (" + action + ")");
                }
            } catch (EOFException e) {
                key.selector().close();
                throw e;
            }
        }

        private ByteBufferBytes readUpTo(int size) throws IOException {

            if (readBuffer.capacity() < size) {
                // resize the buffer
                this.readBuffer = new ByteBufferBytes(ChronicleTcp.createBufferOfSize(size));
            }

            readBuffer.clear();
            readBuffer.buffer().clear();

            readBuffer.limit(size);
            readBuffer.buffer().limit(size);

            connection.readFullyOrEOF(readBuffer.buffer());
            readBuffer.buffer().flip();
            readBuffer.position(0);
            readBuffer.limit(readBuffer.limit());

            return readBuffer;
        }

        protected boolean onWrite(final SelectionKey key) throws IOException {
            final long now = System.currentTimeMillis();
            Object attachment = key.attachment();

            if (running.get() && !write(attachment)) {
                if (lastHeartbeat <= now) {
                    sendSizeAndIndex(ChronicleTcp.IN_SYNC_LEN, 0L);
                }
            }

            return true;
        }

        protected boolean onQuery(final SelectionKey key, long data) throws IOException {
            if (tailer.index(data)) {
                final long now = System.currentTimeMillis();
                setLastHeartbeat(now);

                while (true) {
                    if (tailer.nextIndex()) {
                        sendSizeAndIndex(ChronicleTcp.SYNC_IDX_LEN, tailer.index());
                        break;
                    } else {
                        if (lastHeartbeat <= now) {
                            sendSizeAndIndex(ChronicleTcp.IN_SYNC_LEN, 0L);
                            break;
                        }
                    }
                }
            } else {
                sendSizeAndIndex(ChronicleTcp.IN_SYNC_LEN, 0L);
            }

            return true;
        }

        protected abstract boolean onSubscribe(final SelectionKey key, long data) throws IOException;

        protected abstract boolean write(Object attachment) throws IOException;
    }

    /**
     * IndexedChronicle session handler
     */
    private class IndexedSessionHandler extends SessionHandler {
        private long index;

        private IndexedSessionHandler(final @NotNull SocketChannel socketChannel) {
            super(socketChannel);

            this.index = -1;
        }

        @Override
        protected boolean onSubscribe(final SelectionKey key, long data) throws IOException {
            this.index = data;
            if (this.index == ChronicleTcp.IDX_TO_START) {
                this.index = -1;
            } else if (this.index == ChronicleTcp.IDX_TO_END) {
                this.index = tailer.toEnd().index();
            }

            sendSizeAndIndex(ChronicleTcp.SYNC_IDX_LEN, this.index);

            key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
            return true;
        }

        @Override
        protected boolean write(Object attached) throws IOException {
            if (!tailer.index(index)) {
                if (tailer.wasPadding()) {
                    if (index >= 0) {
                        sendSizeAndIndex(ChronicleTcp.PADDED_LEN, tailer.index());
                    }

                    index++;
                }

                pause();

                if (running.get() && !tailer.index(index)) {
                    return false;
                }
            }

            pauseReset();

            final long size = tailer.capacity();
            long remaining = size + ChronicleTcp.HEADER_SIZE;

            writeBuffer.clear();
            writeBuffer.putInt((int) size);
            writeBuffer.putLong(tailer.index());

            // for large objects send one at a time.
            if (size > writeBuffer.capacity() / 2) {
                while (remaining > 0) {
                    int size2 = (int) Math.min(remaining, writeBuffer.capacity());
                    writeBuffer.limit(size2);
                    tailer.read(writeBuffer);
                    writeBuffer.flip();
                    remaining -= writeBuffer.remaining();

                    connection.writeAll(writeBuffer);
                }
            } else {
                writeBuffer.limit((int) remaining);
                tailer.read(writeBuffer);
                for (int count = builder.maxExcerptsPerMessage(); (count > 0) && tailer.index(index + 1); ) {
                    if (!tailer.wasPadding()) {
                        if (hasRoomForExcerpt(writeBuffer, tailer)) {
                            // if there is free space, copy another one.
                            int size2 = (int) tailer.capacity();
                            writeBuffer.limit(writeBuffer.position() + size2 + ChronicleTcp.HEADER_SIZE);
                            writeBuffer.putInt(size2);
                            writeBuffer.putLong(tailer.index());
                            tailer.read(writeBuffer);

                            index++;
                            count--;
                        } else {
                            break;
                        }
                    } else {
                        index++;
                    }
                }

                writeBuffer.flip();
                connection.writeAll(writeBuffer);
            }

            if (writeBuffer.remaining() > 0) {
                throw new EOFException("Failed to send index=" + index);
            }

            index++;
            return true;
        }
    }

    /**
     * VanillaChronicle session handler
     */
    private class VanillaSessionHandler extends SessionHandler {
        private boolean nextIndex;
        private long index;
        private Bytes withMappedBuffer = new ByteBufferBytes(ByteBuffer.allocate(1024));

        private VanillaSessionHandler(final @NotNull SocketChannel socketChannel) {
            super(socketChannel);

            this.nextIndex = true;
            this.index = -1;
        }

        @Override
        protected boolean onSubscribe(final SelectionKey key, long data) throws IOException {
            this.index = data;
            if (this.index == ChronicleTcp.IDX_TO_START) {
                this.nextIndex = true;
                this.tailer = tailer.toStart();
                this.index = -1;
            } else if (this.index == ChronicleTcp.IDX_TO_END) {
                this.nextIndex = false;
                this.tailer = tailer.toEnd();
                this.index = tailer.index();

                if (this.index == -1) {
                    this.nextIndex = true;
                    this.tailer = tailer.toStart();
                    this.index = -1;
                }
            } else {
                this.nextIndex = false;
            }

            sendSizeAndIndex(ChronicleTcp.SYNC_IDX_LEN, this.index);

            key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
            return false;
        }

        @Override
        protected boolean write(Object attached) throws IOException {
            if (nextIndex) {
                if (!tailer.nextIndex()) {
                    pause();
                    if (running.get() && !tailer.nextIndex()) {
                        return false;
                    }
                }
            } else {
                if (!tailer.index(this.index)) {
                    return false;
                } else {
                    this.nextIndex = true;
                }
            }

            pauseReset();
            Bytes bytes = applyMapping(tailer, attached);

            final long size = bytes.capacity();
            long remaining = size + ChronicleTcp.HEADER_SIZE;

            writeBuffer.clear();
            writeBuffer.putInt((int) size);
            writeBuffer.putLong(tailer.index());

            // for large objects send one at a time.
            if (size > writeBuffer.capacity() / 2)

            {
                while (remaining > 0) {
                    int size2 = (int) Math.min(remaining, writeBuffer.capacity());
                    writeBuffer.limit(size2);
                    bytes.read(writeBuffer);
                    writeBuffer.flip();
                    remaining -= writeBuffer.remaining();
                    connection.writeAll(writeBuffer);
                }
            } else

            {
                writeBuffer.limit((int) remaining);
                bytes.read(writeBuffer);
                for (int count = builder.maxExcerptsPerMessage(); (count > 0) && tailer.nextIndex(); ) {
                    if (!tailer.wasPadding()) {

                        bytes = applyMapping(tailer, attached);

                        if (hasRoomForExcerpt(writeBuffer, bytes)) {
                            // if there is free space, copy another one.
                            int size2 = (int) bytes.capacity();
                            writeBuffer.limit(writeBuffer.position() + size2 + ChronicleTcp.HEADER_SIZE);
                            writeBuffer.putInt(size2);
                            writeBuffer.putLong(tailer.index());
                            bytes.read(writeBuffer);

                            count--;
                        } else {
                            break;
                        }
                    } else {
                        throw new AssertionError("Entry should not be padding - remove");
                    }
                }

                writeBuffer.flip();
                connection.writeAll(writeBuffer);
            }

            if (writeBuffer.remaining() > 0) {
                throw new EOFException("Failed to send index=" + tailer.index());
            }

            return true;
        }


        /**
         * applies a mapping if the mapping is not set to {@code}null{code}
         *
         * @param source the tailer for the mapping to be applied to
         * @param attached the key attachment
         * @return returns the tailer or the mapped bytes
         * @see
         */
        private Bytes applyMapping(final ExcerptTailer source, Object attached) {

            if (attached == null)
                return tailer;

            final MappingProvider mappingProvider = (MappingProvider) attached;
            final MappingFunction mappingFunction = mappingProvider.withMapping();

            if (mappingFunction == null)
                return source;

            withMappedBuffer.clear();


            if (withMappedBuffer.capacity() < source.capacity())
                withMappedBuffer = new ByteBufferBytes(ByteBuffer.allocate((int) source.capacity()));

            try {
                mappingFunction.apply(source, withMappedBuffer);
            } catch (IllegalArgumentException e) {

                // lets try to resize
                if (e.getMessage().contains("Attempt to write")) {

                    if (withMappedBuffer.capacity() == Integer.MAX_VALUE)
                        throw e;

                    int newSize = Math.min(Integer.MAX_VALUE, (int) (withMappedBuffer.capacity() * 1.5));
                    withMappedBuffer = new ByteBufferBytes(ByteBuffer.allocate(newSize));
                } else
                    throw e;

            }
            withMappedBuffer.flip();

            // set the capacity() equal withMappedBuffer the limit()
            return withMappedBuffer.slice(0, withMappedBuffer.limit());

        }
    }
}
