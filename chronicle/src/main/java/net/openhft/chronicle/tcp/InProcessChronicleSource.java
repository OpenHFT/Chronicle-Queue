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
import net.openhft.lang.thread.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A Chronicle as a service to be replicated to any number of clients.
 * Clients can restart from where ever they are up to.
 *
 * <p>Can be used an in process component which wraps the underlying Chronicle and offers
 * lower overhead than using ChronicleSource
 *
 * @author peter.lawrey
 */
public class InProcessChronicleSource extends ChronicleSource {

    private static final long HEARTBEAT_INTERVAL_MS = 2500;
    private static final int MAX_MESSAGE = 128;
    @NotNull
    private final Chronicle chronicle;
    private final ServerSocketChannel server;
    private final Selector selector;
    @NotNull
    private final String name;
    @NotNull
    private final ExecutorService service;
    private final Logger logger;
    private final Object notifier = new Object();
    private static final long busyWaitTimeNS = 100 * 1000;
    private volatile boolean closed = false;
    private long lastUnpausedNS = 0;

    public InProcessChronicleSource(@NotNull Chronicle chronicle, int port) throws IOException {
        this.chronicle = chronicle;
        server = ServerSocketChannel.open();
        server.socket().setReuseAddress(true);
        server.socket().bind(new InetSocketAddress(port));
        server.configureBlocking(false);
        selector = Selector.open();
        server.register(selector, SelectionKey.OP_ACCEPT);
        name = chronicle.name() + "@" + port;
        logger = LoggerFactory.getLogger(getClass().getName() + "." + name);
        service = Executors.newCachedThreadPool(new NamedThreadFactory(name, true));
        service.execute(new Acceptor());
    }

    private void pauseReset() {
        lastUnpausedNS = System.nanoTime();
    }

    void pause() {
        if (lastUnpausedNS + busyWaitTimeNS > System.nanoTime())
            return;
        try {
            synchronized (notifier) {
                notifier.wait(HEARTBEAT_INTERVAL_MS / 2);
            }
        } catch (InterruptedException ie) {
            logger.warn("Interrupt ignored");
        }
    }

    void wakeSessionHandlers() {
        synchronized (notifier) {
            notifier.notifyAll();
        }
    }

    @Override
    public String name() {
        return chronicle.name();
    }

    @NotNull
    @Override
    public Excerpt createExcerpt() throws IOException {
        return new SourceExcerpt(chronicle.createExcerpt());
    }

    @NotNull
    @Override
    public ExcerptTailer createTailer() throws IOException {
        return new SourceExcerpt(chronicle.createTailer());
    }

    @NotNull
    @Override
    public ExcerptAppender createAppender() throws IOException {
        return new SourceExcerpt(chronicle.createAppender());
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
        try {
            chronicle.close();
            server.close();
            service.shutdownNow();
            service.awaitTermination(10000, java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (IOException e) {
            logger.warn("Error closing server port", e);
        } catch (InterruptedException ie) {
            logger.warn("Error shutting down service threads", ie);
        }
    }

    public ChronicleConfig config() {
        return ((IndexedChronicle) chronicle).config();
    }

    private class Acceptor implements Runnable {
        @Override
        public void run() {
            Thread.currentThread().setName(name + "-acceptor");
            try {
                while (!closed) {
                    selector.select();
                    Set<SelectionKey> keys = selector.keys();
                    for (SelectionKey key : keys) {
                        if (key.isAcceptable()) {
                            SocketChannel socket = server.accept();
                            socket.configureBlocking(true);
                            service.execute(new Handler(socket));
                        }
                    }
                }
            } catch (IOException e) {
                if (!closed) {
                    logger.warn("Acceptor dying", e);
                }
            } finally {
                service.shutdown();
                logger.info("Acceptor loop ended");
            }
        }
    }

    class Handler implements Runnable {
        private final SocketChannel socket;
        private final Selector selector;
        private final ExcerptTailer tailer;
        private final ByteBuffer buffer;

        private long index;
        private long lastHeartbeatTime;

        public Handler(@NotNull SocketChannel socket) throws IOException {
            this.socket = socket;
            this.socket.configureBlocking(false);
            this.socket.socket().setSendBufferSize(256 * 1024);
            this.socket.socket().setTcpNoDelay(true);
            this.selector = Selector.open();
            this.tailer = chronicle.createTailer();
            this.buffer = TcpUtil.createBuffer(1, ByteOrder.nativeOrder());
            this.index = -1;
            this.lastHeartbeatTime = 0;
        }

        private boolean write() throws IOException {
            if (!tailer.index(index)) {
                if (tailer.wasPadding()) {
                    if (index >= 0) {
                        buffer.clear();
                        buffer.putInt(PADDED_LEN);
                        buffer.putLong(tailer.index());
                        buffer.flip();
                        TcpUtil.writeAll(socket, buffer);
                    }

                    index++;
                }

                pause();

                if(!closed && !tailer.index(index)) {
                    return false;
                }
            }

            pauseReset();

            final long size = tailer.capacity();
            long remaining = size + TcpUtil.HEADER_SIZE;

            buffer.clear();
            buffer.putInt((int) size);
            buffer.putLong(tailer.index());

            // for large objects send one at a time.
            if (size > buffer.capacity() / 2) {
                while (remaining > 0) {
                    int size2 = (int) Math.min(remaining, buffer.capacity());
                    buffer.limit(size2);
                    tailer.read(buffer);
                    buffer.flip();
                    remaining -= buffer.remaining();
                    TcpUtil.writeAll(socket, buffer);
                }
            } else {
                buffer.limit((int) remaining);
                tailer.read(buffer);
                int count = 1;
                while (tailer.index(index + 1) && count++ < MAX_MESSAGE) {
                    if(!tailer.wasPadding()) {
                        if (tailer.capacity() + TcpUtil.HEADER_SIZE >= (buffer.capacity() - buffer.position())) {
                            break;
                        }

                        // if there is free space, copy another one.
                        int size2 = (int) tailer.capacity();
                        buffer.limit(buffer.position() + size2 + TcpUtil.HEADER_SIZE);
                        buffer.putInt(size2);
                        buffer.putLong(tailer.index());
                        tailer.read(buffer);
                        index++;
                    } else {
                        index++;
                    }
                }

                buffer.flip();
                TcpUtil.writeAll(socket, buffer);
            }

            if (buffer.remaining() > 0) {
                throw new EOFException("Failed to send index=" + index);
            }

            index++;
            return true;
        }

        @Override
        public void run() {
            try {
                socket.register(selector, SelectionKey.OP_READ);

                while(!closed) {
                    if(selector.select() > 0) {
                        final Set<SelectionKey> keys = selector.selectedKeys();
                        final Iterator<SelectionKey> it = keys.iterator();

                        while (it.hasNext()) {
                            final SelectionKey key = it.next();
                            it.remove();

                            if(key.isReadable()) {
                                try {
                                    this.index = readIndex(socket);
                                    if(this.index == -1) {
                                        this.index = 0;
                                    } else if(this.index == -2){
                                        this.index = tailer.toEnd().index();
                                    }

                                    buffer.clear();
                                    buffer.putInt(SYNC_IDX_LEN);
                                    buffer.putLong(this.index);
                                    buffer.flip();
                                    TcpUtil.writeAll(socket, buffer);

                                    this.lastHeartbeatTime = System.currentTimeMillis() + HEARTBEAT_INTERVAL_MS;

                                    key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                                    keys.clear();
                                    break;
                                } catch(EOFException e) {
                                    key.selector().close();
                                    throw e;
                                }
                            } else if(key.isWritable()) {
                                final long now = System.currentTimeMillis();
                                if(!closed && !write()) {
                                    if (lastHeartbeatTime <= now) {
                                        buffer.clear();
                                        buffer.putInt(IN_SYNC_LEN);
                                        buffer.putLong(0L);
                                        buffer.flip();
                                        TcpUtil.writeAll(socket, buffer);
                                        lastHeartbeatTime = now + HEARTBEAT_INTERVAL_MS;
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                if (!closed) {
                    String msg = e.getMessage();
                    if (msg != null &&
                            (msg.contains("reset by peer")
                                || msg.contains("Broken pipe")
                                || msg.contains("was aborted by"))) {
                        logger.info("Connect {} closed from the other end", socket, e);
                    } else {
                        logger.info("Connect {} died",socket, e);
                    }

                    try {
                        this.socket.close();
                    } catch(IOException ioe) {
                        logger.warn("",e);
                    }
                }
            }
        }

        private long readIndex(@NotNull SocketChannel socket) throws IOException {
            ByteBuffer bb = ByteBuffer.allocate(8);
            TcpUtil.readFullyOrEOF(socket, bb);
            return bb.getLong(0);
        }

    }

    class SourceExcerpt extends WrappedExcerpt {
        public SourceExcerpt(ExcerptCommon excerptCommon) {
            super(excerptCommon);
        }

        @Override
        public void finish() {
            super.finish();
            wakeSessionHandlers();
//            System.out.println("Wrote " + index());
        }
    }
}
