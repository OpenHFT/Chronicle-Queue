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
import net.openhft.chronicle.tools.ResizableDirectByteBufferBytes;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.DirectByteBufferBytes;
import net.openhft.lang.model.constraints.NotNull;
import net.openhft.lang.thread.LightPauser;
import org.jetbrains.annotations.Nullable;
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
    protected final LightPauser pauser;

    protected SourceTcp(String name, final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder, ThreadPoolExecutor executor) {
        this.builder = builder;
        this.name = ChronicleTcp.connectionName(name, this.builder.bindAddress(), this.builder.connectAddress());
        this.logger = LoggerFactory.getLogger(this.name);
        this.running = new AtomicBoolean(false);
        this.executor = executor;
        this.pauser = new LightPauser(builder.busyPeriodTimeNanos(), builder.parkPeriodTimeNanos());
    }

    public SourceTcp open() {
        this.running.set(true);
        this.executor.execute(createHandler());

        return this;
    }

    public boolean close() {
        running.set(false);
        executor.shutdown();

        try {
            executor.awaitTermination(
                    builder.selectTimeout() * 2,
                    builder.selectTimeoutUnit());
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }

        return !running.get();
    }

    @Override
    public String toString() {
        return this.name;
    }

    public void dataNotification() {
        pauser.unpause();
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
                builder.connectionListener().onConnect(socketChannel);
                return new IndexedSessionHandler(socketChannel);

            } else if (chronicle instanceof VanillaChronicle) {
                builder.connectionListener().onConnect(socketChannel);
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
     * Abstract class for Indexed and Vanilla chronicle replication
     */
    private abstract class SessionHandler implements Runnable, Closeable {
        protected final TcpConnection connection;
        protected final ByteBuffer writeBuffer;
        protected final ResizableDirectByteBufferBytes readBuffer;
        private final SocketChannel socketChannel;
        protected ExcerptTailer tailer;
        protected ExcerptAppender appender;
        protected long lastHeartbeat;
        private long lastUnPausedNS;
        private ResizableDirectByteBufferBytes withMappedBuffer;

        private SessionHandler(final @NotNull SocketChannel socketChannel) {
            this.socketChannel = socketChannel;
            this.connection = new TcpConnection(socketChannel);
            this.tailer = null;
            this.appender = null;
            this.lastHeartbeat = 0;
            this.lastUnPausedNS = 0;

            this.readBuffer = new ResizableDirectByteBufferBytes(16);
            this.readBuffer.clearThreadAssociation();
            this.writeBuffer = ChronicleTcp.createBuffer(builder.minBufferSize());
            this.writeBuffer.limit(0);

            this.withMappedBuffer = new ResizableDirectByteBufferBytes(1024);
        }

        @Override
        public void close() throws IOException {
            if (this.tailer != null) {
                this.tailer.close();
                this.tailer = null;
            }

            if (this.appender != null) {
                this.appender.close();
                this.appender = null;
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
                socketChannel.socket().setTcpNoDelay(true);
                socketChannel.socket().setSoTimeout(0);
                socketChannel.socket().setSoLinger(false, 0);

                if(builder.receiveBufferSize() > 0) {
                    socketChannel.socket().setReceiveBufferSize(builder.receiveBufferSize());
                }
                if(builder.sendBufferSize() > 0) {
                    socketChannel.socket().setSendBufferSize(builder.sendBufferSize());
                }

                final VanillaSelector selector = new VanillaSelector()
                        .open()
                        .register(socketChannel, SelectionKey.OP_READ, new Attached());

                tailer   = builder.chronicle().createTailer();
                appender = builder.chronicle().createAppender();

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
            final int  spinLoopCount = builder.selectorSpinLoopCount();
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
            final int  spinLoopCount = builder.selectorSpinLoopCount();
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
            return hasRoomFor(buffer, tailer.remaining() + ChronicleTcp.HEADER_SIZE);
        }

        protected boolean hasRoomFor(ByteBuffer buffer, long size) {
            return buffer.remaining() >= size;
        }

        protected void pauseReset() {
            lastUnPausedNS = System.nanoTime();
            pauser.reset();
        }

        protected void pause() {
            if (lastUnPausedNS + ChronicleTcp.BUSY_WAIT_TIME_NS > System.nanoTime()) {
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
            connection.writeSizeAndIndex(writeBuffer, size, index);
            setLastHeartbeat();
        }

        protected DirectByteBufferBytes readUpTo(int size) throws IOException {
            readBuffer.resetToSize(size);
            connection.readFullyOrEOF(readBuffer.buffer());
            readBuffer.buffer().flip();
            readBuffer.position(0);
            readBuffer.limit(readBuffer.limit());

            return readBuffer;
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
                        return onMapping(key, readUpTo(4).readInt());
                    case (int) ChronicleTcp.ACTION_SUBSCRIBE:
                        return onSubscribe(key, readUpTo(8).readLong());
                    case (int) ChronicleTcp.ACTION_UNSUBSCRIBE:
                        return onUnsubscribe(key, readUpTo(8).readLong());
                    case (int) ChronicleTcp.ACTION_QUERY:
                        return onQuery(key, readUpTo(8).readLong());
                    case (int) ChronicleTcp.ACTION_SUBMIT:
                        return onSubmit(key, readUpTo(8).readLong(), true);
                    case (int) ChronicleTcp.ACTION_SUBMIT_NOACK:
                        return onSubmit(key, readUpTo(8).readLong(), false);
                    default:
                        throw new IOException("Unknown action received (" + action + ")");
                }
            } catch (IOException e) {
                key.selector().close();
                throw e;
            } 
        }
        protected boolean onWrite(final SelectionKey key) throws IOException {
            final long now = System.currentTimeMillis();
            final Object attachment = key.attachment();

            if (running.get() && !write(attachment)) {
                if (lastHeartbeat <= now) {
                    sendSizeAndIndex(ChronicleTcp.IN_SYNC_LEN, ChronicleTcp.IDX_NONE);
                }
            }

            return true;
        }

        protected boolean onMapping(final SelectionKey key, int size) throws IOException {
            MappingProvider mappingProvider = (MappingProvider)key.attachment();
            if (mappingProvider != null) {
                MappingFunction mappingFunction = readUpTo(size).readObject(MappingFunction.class);
                mappingProvider.withMapping(mappingFunction);
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
                        tailer.finish();
                        break;

                    } else {
                        if (lastHeartbeat <= now) {
                            sendSizeAndIndex(ChronicleTcp.IN_SYNC_LEN, ChronicleTcp.IDX_NONE);
                            break;
                        }
                    }
                }
            } else {
                sendSizeAndIndex(ChronicleTcp.IN_SYNC_LEN, 0L);
            }

            return true;
        }

        protected boolean onUnsubscribe(final SelectionKey key, long data) {
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
            return true;
        }

        protected abstract boolean onSubscribe(final SelectionKey key, long data) throws IOException ;
        protected abstract boolean onSubmit(final SelectionKey key, long size, boolean ack) throws IOException ;
        protected abstract boolean write(Object attachment) throws IOException;

        /**
         * applies a mapping if the mapping is not set to {@code}null{code}
         *
         * @param source the tailer for the mapping to be applied to
         * @param attached the key attachment
         * @return returns the tailer or the mapped bytes
         * @see
         */
        protected Bytes applyMapping(@NotNull final ExcerptTailer source,
                                     @Nullable Object attached) {
            if (attached == null) {
                return source;
            }

            final MappingProvider mappingProvider = (MappingProvider) attached;
            final MappingFunction mappingFunction = mappingProvider.withMapping();
            if (mappingFunction == null) {
                return source;
            }

            withMappedBuffer.clear();
            if (withMappedBuffer.capacity() < source.limit()) {
                withMappedBuffer.resetToSize((int)source.capacity());
            }

            try {
                mappingFunction.apply(source, withMappedBuffer);
            } catch (IllegalArgumentException e) {
                // lets try to resize
                if (e.getMessage().contains("Attempt to write")) {
                    if (withMappedBuffer.capacity() == Integer.MAX_VALUE) {
                        throw e;
                    }

                    withMappedBuffer.resetToSize(
                        Math.min(
                            Integer.MAX_VALUE,
                            (int) (withMappedBuffer.capacity() * 1.5))
                    );

                } else {
                    throw e;
                }
            }

            return withMappedBuffer.flip();
        }
    }

    // *************************************************************************
    // SessionHandler - implementations
    // *************************************************************************

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
        protected boolean onSubmit(final SelectionKey key, long size, boolean ack) throws IOException {
            if(ack) {
                sendSizeAndIndex(ChronicleTcp.NACK_LEN, ChronicleTcp.IDX_NOT_SUPPORTED);
            }

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

            Bytes bytes = applyMapping(tailer, attached);
            int size = (int)bytes.limit();

            writeBuffer.clear();
            writeBuffer.putInt(size);
            writeBuffer.putLong(tailer.index());

            // for large objects send one at a time.
            if (size > writeBuffer.capacity() / 2) {
                while (size > 0) {
                    int minSize = Math.min(size, writeBuffer.remaining());
                    bytes.read(writeBuffer, minSize);
                    writeBuffer.flip();
                    connection.writeAll(writeBuffer);

                    size -= minSize;
                    if(size > 0) {
                        writeBuffer.clear();
                    }
                }
            } else {
                bytes.read(writeBuffer, size);
                for (int count = builder.maxExcerptsPerMessage(); (count > 0) && tailer.index(index + 1); ) {
                    if (!tailer.wasPadding()) {
                        bytes = applyMapping(tailer, attached);
                        // if there is free space, copy another one.
                        if (hasRoomForExcerpt(writeBuffer, bytes)) {
                            size = (int) bytes.limit();
                            writeBuffer.putInt(size);
                            writeBuffer.putLong(tailer.index());
                            bytes.read(writeBuffer, size);

                            index++;
                            count--;

                            tailer.finish();

                        } else {
                            break;
                        }
                    } else {
                        if(hasRoomFor(writeBuffer, ChronicleTcp.HEADER_SIZE)) {
                            writeBuffer.putInt(ChronicleTcp.PADDED_LEN);
                            writeBuffer.putLong(index);

                        } else {
                            break;
                        }

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
        protected boolean onSubmit(final SelectionKey key, long size, boolean ack) throws IOException {
            readUpTo((int) size);

            appender.startExcerpt((int) size);
            appender.write(readBuffer);
            appender.finish();

            pauser.unpause();

            if(ack) {
                sendSizeAndIndex(ChronicleTcp.ACK_LEN, appender.lastWrittenIndex());
            }

            return true;
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
            int size = (int)bytes.limit();

            writeBuffer.clear();
            writeBuffer.putInt(size);
            writeBuffer.putLong(tailer.index());

            // for large objects send one at a time.
            if (size > writeBuffer.limit() / 2) {
                while (size > 0) {
                    int minSize = Math.min(size, writeBuffer.remaining());
                    bytes.read(writeBuffer, minSize);
                    writeBuffer.flip();
                    connection.writeAll(writeBuffer);
                    writeBuffer.clear();

                    size -= minSize;
                    if(size > 0) {
                        writeBuffer.clear();
                    }
                }
            } else {
                bytes.read(writeBuffer, size);

                long previousIndex = tailer.index();
                long currentIndex = previousIndex;
                for (int count = builder.maxExcerptsPerMessage(); (count > 0) && tailer.nextIndex(); ) {
                    currentIndex = tailer.index();
                    bytes = applyMapping(tailer, attached);

                    // if there is free space, copy another one.
                    if (hasRoomForExcerpt(writeBuffer, bytes)) {
                        size = (int)bytes.limit();
                        previousIndex = currentIndex;
                        writeBuffer.putInt(size);
                        writeBuffer.putLong(currentIndex);

                        bytes.read(writeBuffer, size);
                        count--;

                        tailer.finish();

                    } else {
                        tailer.finish();
                        // if there is no space, go back to the previous index
                        tailer.index(previousIndex);
                        break;
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
    }
}
