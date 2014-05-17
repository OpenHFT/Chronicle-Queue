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
import net.openhft.lang.thread.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A Chronicle as a service to be replicated to any number of clients.
 * Clients can restart from where ever they are up to.
 *
 * <p>Can be used an in process component which wraps the underlying Chronicle
 * and offers lower overhead than using ChronicleSource
 *
 * @author peter.lawrey
 */
public class VanillaChronicleSource implements Chronicle {
    static final int IN_SYNC_LEN = -128;
    static final int PADDED_LEN = -127;
    private static final long HEARTBEAT_INTERVAL_MS = 2500;
    private static final int MAX_MESSAGE = 128;
    @NotNull
    private final VanillaChronicle chronicle;
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

    public VanillaChronicleSource(@NotNull VanillaChronicle chronicle, int port) throws IOException {
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

    public int getLocalPort() {
        return server.socket().getLocalPort();
    }

    public void clear() {
        chronicle.clear();
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
        throw new UnsupportedOperationException();
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
        @NotNull
        private final SocketChannel socket;

        public Handler(@NotNull SocketChannel socket) throws SocketException {
            this.socket = socket;
            socket.socket().setSendBufferSize(256 * 1024);
            socket.socket().setTcpNoDelay(true);
        }

        @Override
        public void run() {
            try {
                long lastSinkIndex = readIndex(socket);
                ExcerptTailer excerpt = chronicle.createTailer();
                ByteBuffer bb = TcpUtil.createBuffer(1, ByteOrder.nativeOrder()); // minimum size
                long sendInSync = 0;
                excerpt.index(lastSinkIndex);
                OUTER:
                while (!closed) {
                    while (!excerpt.nextIndex()) {
                        long now = System.currentTimeMillis();
                        if (sendInSync <= now) {
                            bb.clear();
                            //The sink is expecting this structure long for index then int for size
                            bb.putLong(IN_SYNC_LEN);
                            bb.putInt(IN_SYNC_LEN);
                            bb.flip();
                            TcpUtil.writeAll(socket, bb);
                            sendInSync = now + HEARTBEAT_INTERVAL_MS;
                        }

                        pause();
                        if (closed) break OUTER;
                    }
                    pauseReset();
                    final long size = excerpt.capacity();
                    long remaining;

                    bb.clear();
                    //8 bytes for the index (a long) 4 bytes for size (an int)
                    remaining = size + 8 + 4;
                    bb.putLong(excerpt.index());
                    bb.putInt((int) size);

                    // for large objects send one at a time.
                    if (size > bb.capacity() / 2) {
                        while (remaining > 0) {
                            int size2 = (int) Math.min(remaining, bb.capacity());
                            bb.limit(size2);
                            excerpt.read(bb);
                            bb.flip();
                            remaining -= bb.remaining();
                            TcpUtil.writeAll(socket, bb);
                        }
                    } else {
                        bb.limit((int) remaining);
                        excerpt.read(bb);
                        int count = 1;
                        while (count++ < MAX_MESSAGE) {
                            if (excerpt.nextIndex()) {
                                if (excerpt.wasPadding()) {
                                    throw new AssertionError("Entry should not be padding - remove");
                                }
                                if (excerpt.remaining() + 4 + 8 >= bb.capacity() - bb.position())
                                    break;
                                // if there is free space, copy another one.
                                int size2 = (int) excerpt.capacity();
                                bb.limit(bb.position() + size2 + 4 + 8);
                                bb.putLong(excerpt.index());
                                bb.putInt(size2);
                                excerpt.read(bb);
                            }
                        }

                        bb.flip();
                        TcpUtil.writeAll(socket, bb);
                    }
                    if (bb.remaining() > 0) throw new EOFException("Failed to send index=" + excerpt.index());

                    sendInSync = 0;
                }
            } catch (Exception e) {
                if (!closed) {
                    String msg = e.getMessage();
                    if (msg != null &&
                            (msg.contains("reset by peer")
                                || msg.contains("Broken pipe")
                                || msg.contains("was aborted by"))) {
                        logger.info("Connect {} closed from the other end ", socket, e);
                    } else {
                        logger.info("Connect {} died", socket, e);
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
        }
    }
}