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
package net.openhft.chronicle.tcp2;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.IndexedChronicle;
import net.openhft.chronicle.VanillaChronicle;
import net.openhft.lang.model.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class SourceTcp {
    protected final Logger logger;
    protected final String name;
    protected final AtomicBoolean running;
    protected final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder;
    protected final ThreadPoolExecutor executor;

    protected final ByteBuffer writeBuffer;
    protected final ByteBuffer readBuffer;

    protected SourceTcp(String name, final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder, ThreadPoolExecutor executor) {
        this.builder = builder;
        this.name = ChronicleTcp2.connectionName(name, this.builder.bindAddress(), this.builder.connectAddress());
        this.logger = LoggerFactory.getLogger(this.name);
        this.running = new AtomicBoolean(false);
        this.executor = executor;
        this.readBuffer = ChronicleTcp2.createBuffer(16);
        this.writeBuffer = ChronicleTcp2.createBuffer(builder.minBufferSize());
    }

    public boolean open() throws IOException {
        this.running.set(true);

        readBuffer.clear();
        readBuffer.limit(16);

        writeBuffer.clear();
        writeBuffer.limit(0);

        this.executor.execute(createHandler());

        return this.running.get();
    }

    public boolean close()  throws IOException {
        running.set(false);
        executor.shutdown();

        try {
            executor.awaitTermination(
                builder.selectTimeout() * 2,
                builder.selectTimeoutUnit());
        } catch(InterruptedException e) {
            // Ignored
        }

        return !running.get();
    }

    @Override
    public String toString() {
        return this.name;
    }

    protected abstract Runnable createHandler();

    /**
     * Creates a session handler according to the Chronicle the sources is connected to.
     *
     * @param socketChannel     The {@link java.nio.channels.SocketChannel}
     * @return                  The Runnable
     */
    protected Runnable createSessionHandler(final @NotNull SocketChannel socketChannel) {
        final Chronicle chronicle = builder.chronicle();
        if(chronicle != null) {
            if(chronicle instanceof IndexedChronicle) {
                return new IndexedSessionHandler(socketChannel);
            } else if(chronicle instanceof VanillaChronicle) {
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

        protected ExcerptTailer tailer;

        private SessionHandler(final @NotNull SocketChannel socketChannel) {
            this.socketChannel = socketChannel;
            this.tailer = null;
        }

        @Override
        public void close() throws IOException {
            if(this.tailer != null) {
                this.tailer.close();
                this.tailer = null;
            }

            if(this.socketChannel.isOpen()) {
                this.socketChannel.close();
            }
        }

        @Override
        public void run() {
            try {
                socketChannel.configureBlocking(false);
                socketChannel.socket().setSendBufferSize(builder.minBufferSize());
                socketChannel.socket().setTcpNoDelay(true);

                Selector selector = Selector.open();

                tailer = builder.chronicle().createTailer();
                socketChannel.register(selector, SelectionKey.OP_READ);

                while(!running.get() && !Thread.currentThread().isInterrupted()) {
                    if (selector.select(builder.selectTimeoutMillis()) > 0) {
                        final Set<SelectionKey> keys = selector.selectedKeys();
                        for (final Iterator<SelectionKey> it = keys.iterator(); it.hasNext();) {
                            final SelectionKey key = it.next();
                            if(key.isReadable()) {
                                if (!onRead(key)) {
                                    keys.clear();
                                    break;
                                } else {
                                    it.remove();
                                }
                            } else if(key.isWritable()) {
                                if (!onWrite(key)) {
                                    keys.clear();
                                    break;
                                } else {
                                    it.remove();
                                }
                            } else {
                                it.remove();
                            }
                        }
                    }
                }
            } catch (EOFException e) {
                if (!running.get()) {
                    logger.info("Connection {} died", socketChannel);
                }
            } catch (Exception e) {
                if (!running.get()) {
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
            }

            try {
                close();
            } catch(IOException e) {
                logger.warn("",e);
            }
        }

        protected boolean onRead(final SelectionKey key) throws IOException {
            /*
            try {

                command.read(socket);

                if(command.isSubscribe()) {
                    return handleSubscribe(key);
                } else if(command.isQuery()) {
                    return handleQuery(key);
                } else {
                    throw new IOException("Unknown action received (" + command.action() + ")");
                }
            } catch(EOFException e) {
                key.selector().close();
                throw e;
            }
            */
            return true;
        }

        protected boolean onWrite(final SelectionKey key) throws IOException {
            /*
            final long now = System.currentTimeMillis();
            if(!this.source.closed() && !publishData()) {
                if (lastHeartbeat <= now) {
                    sendSizeAndIndex(ChronicleTcp.IN_SYNC_LEN, 0L);
                }
            }
            */

            return true;
        }

        protected boolean onSubscribe(final SelectionKey key) throws IOException {
            return false;
        }

        protected boolean onQuery(final SelectionKey key) throws IOException {
            return false;
        }
    }

    /**
     * IndexedChronicle session handler
     */
    private class IndexedSessionHandler extends SessionHandler {
        private IndexedSessionHandler(final @NotNull SocketChannel socketChannel) {
            super(socketChannel);
        }

        @Override
        public void run() {
        }
    }

    /**
     * VanillaChronicle session handler
     */
    private class VanillaSessionHandler extends SessionHandler {
        private VanillaSessionHandler(final @NotNull SocketChannel socketChannel) {
            super(socketChannel);
        }

        @Override
        public void run() {
        }
    }
}
