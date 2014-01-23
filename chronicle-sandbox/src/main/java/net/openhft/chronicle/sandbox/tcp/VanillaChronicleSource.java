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

import net.openhft.chronicle.sandbox.VanillaChronicle;
import net.openhft.chronicle.tcp.TcpUtil;
import net.openhft.chronicle.tools.WrappedExcerpt;
import net.openhft.lang.thread.NamedThreadFactory;
import org.jetbrains.annotations.NotNull;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public class VanillaChronicleSource implements Chronicle {
    static final int IN_SYNC_LEN = -128;
    static final int PADDED_LEN = -127;

    static final long HEARTBEAT_INTERVAL_MS = 2500;
    private static final int MAX_MESSAGE = 128;

    private final VanillaChronicle chronicle;
    private final ServerSocketChannel server;
    @NotNull
    private final String name;
    @NotNull
    private final ExecutorService service;
    private final Logger logger;
    private final Object notifier = new Object();
    private long busyWaitTimeNS = 100 * 1000;
    private volatile boolean closed = false;
    private long lastUnpausedNS = 0;

    public VanillaChronicleSource(@NotNull VanillaChronicle chronicle, int port) throws IOException {
        this.chronicle = chronicle;
        server = ServerSocketChannel.open();
        server.socket().setReuseAddress(true);
        server.socket().bind(new InetSocketAddress(port));
        name = chronicle.name() + "@" + port;
        logger = Logger.getLogger(getClass().getName() + "." + name);
        service = Executors.newCachedThreadPool(new NamedThreadFactory(name, true));
        service.execute(new Acceptor());
    }

    private void pauseReset() {
        lastUnpausedNS = System.nanoTime();
    }

    protected void pause() {
        if (lastUnpausedNS + busyWaitTimeNS > System.nanoTime())
            return;
        try {
            synchronized (notifier) {
                notifier.wait(HEARTBEAT_INTERVAL_MS / 2);
            }
        } catch (InterruptedException ie) {
            logger.warning("Interrupt ignored");
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
    public void close() {
        closed = true;
        try {
            chronicle.close();
            server.close();
        } catch (IOException e) {
            logger.warning("Error closing server port " + e);
        }
    }

    public ChronicleConfig config() {
        throw new UnsupportedOperationException();
    }

    public int getLocalPort() {
        return server.socket().getLocalPort();
    }

    public void clear() {
        chronicle.clear();
    }

    private class Acceptor implements Runnable {
        @Override
        public void run() {
            Thread.currentThread().setName(name + "-acceptor");
            try {
                while (!closed) {
                    SocketChannel socket = server.accept();
                    service.execute(new Handler(socket));
                }
            } catch (IOException e) {
                if (!closed)
                    logger.log(Level.SEVERE, "Acceptor dying", e);
            } finally {
                service.shutdown();
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
                long index = readIndex(socket);
                ExcerptTailer excerpt = chronicle.createTailer();
                ByteBuffer bb = TcpUtil.createBuffer(1, ByteOrder.nativeOrder()); // minimum size
                long sendInSync = 0;
                boolean first = true;
                OUTER:
                while (!closed) {
                    while (!excerpt.index(index)) {
                        long now = System.currentTimeMillis();
                        if (excerpt.wasPadding()) {
                            if (index >= 0) {
                                bb.clear();
                                if (first) {
                                    bb.putLong(excerpt.index());
                                    first = false;
                                }
                                bb.putInt(PADDED_LEN);
                                bb.flip();
                                TcpUtil.writeAll(socket, bb);
                                sendInSync = now + HEARTBEAT_INTERVAL_MS;
                            }
                            index++;
                            continue;
                        }
//                        System.out.println("Waiting for " + index);
                        if (sendInSync <= now && !first) {
                            bb.clear();
                            bb.putInt(IN_SYNC_LEN);
                            bb.flip();
                            TcpUtil.writeAll(socket, bb);
                            sendInSync = now + HEARTBEAT_INTERVAL_MS;
                        }
                        pause();
                        if (closed) break OUTER;
                    }
                    pauseReset();
//                    System.out.println("Writing " + index);
                    final long size = excerpt.capacity();
                    long remaining;

                    bb.clear();
                    if (first) {
//                        System.out.println("wi " + index);
                        bb.putLong(excerpt.index());
                        first = false;
                        remaining = size; // + TcpUtil.HEADER_SIZE;
                    } else {
                        remaining = size + 4;
                    }
                    bb.putInt((int) size);
                    // for large objects send one at a time.
                    if (size > bb.capacity() / 2) {
                        while (remaining > 0) {
                            int size2 = (int) Math.min(remaining, bb.capacity());
                            bb.limit(size2);
                            excerpt.read(bb);
                            bb.flip();
//                        System.out.println("w " + ChronicleTools.asString(bb));
                            remaining -= bb.remaining();
                            TcpUtil.writeAll(socket, bb);
                        }
                    } else {
                        bb.limit((int) remaining);
                        excerpt.read(bb);
                        int count = 1;
                        while (excerpt.index(index + 1) && count++ < MAX_MESSAGE) {
                            if (excerpt.wasPadding()) {
                                index++;
                                continue;
                            }
                            if (excerpt.remaining() + 4 >= bb.capacity() - bb.position())
                                break;
                            // if there is free space, copy another one.
                            int size2 = (int) excerpt.capacity();
//                            System.out.println("W+ "+size);
                            bb.limit(bb.position() + size2 + 4);
                            bb.putInt(size2);
                            excerpt.read(bb);

                            index++;
                        }

                        bb.flip();
//                        System.out.println("W " + size + " wb " + bb);
                        TcpUtil.writeAll(socket, bb);
                    }
                    if (bb.remaining() > 0) throw new EOFException("Failed to send index=" + index);
                    index++;
                    sendInSync = 0;
//                    if (index % 20000 == 0)
//                        System.out.println(System.currentTimeMillis() + ": wrote " + index);
                }
            } catch (Exception e) {
                if (!closed) {
                    String msg = e.getMessage();
                    if (msg != null &&
                            (msg.contains("reset by peer") || msg.contains("Broken pipe")
                                    || msg.contains("was aborted by")))
                        logger.log(Level.INFO, "Connect " + socket + " closed from the other end " + e);
                    else
                        logger.log(Level.INFO, "Connect " + socket + " died", e);
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
