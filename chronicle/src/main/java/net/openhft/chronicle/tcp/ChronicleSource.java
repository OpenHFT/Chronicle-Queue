/*
 * Copyright 2014 Higher Frequency Trading
 * <p/>
 * http://www.higherfrequencytrading.com
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.tcp;

import net.openhft.chronicle.*;
import net.openhft.chronicle.tools.WrappedExcerpt;
import net.openhft.lang.model.constraints.NotNull;
import net.openhft.lang.thread.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ChronicleSource implements Chronicle {
    @NotNull
    private final Chronicle chronicle;
    private final ChronicleSourceConfig config;
    private final ServerSocketChannel server;
    private final Selector selector;
    @NotNull
    private final String name;
    @NotNull
    private final ExecutorService service;
    private final Logger logger;
    private final Object notifier = new Object();
    private static final long busyWaitTimeNS = 100 * 1000;
    private volatile boolean closed;
    private long lastUnpausedNS = 0;
    private int maxMessages;

    public ChronicleSource(@NotNull final Chronicle chronicle, final int port) throws IOException {
        this(chronicle, ChronicleSourceConfig.DEFAULT, new InetSocketAddress(port));
    }

    public ChronicleSource(@NotNull final Chronicle chronicle, @NotNull final InetSocketAddress address) throws IOException {
        this(chronicle, ChronicleSourceConfig.DEFAULT, address);
    }

    public ChronicleSource(@NotNull final Chronicle chronicle, @NotNull final ChronicleSourceConfig config, final int port) throws IOException {
        this(chronicle, config, new InetSocketAddress(port));
    }

    public ChronicleSource(@NotNull final Chronicle chronicle, @NotNull final ChronicleSourceConfig config, @NotNull final InetSocketAddress address) throws IOException {
        this.closed = false;
        this.chronicle = chronicle;
        this.config = config;
        this.server = ServerSocketChannel.open();
        this.server.socket().setReuseAddress(true);
        this.server.socket().bind(address);
        this.server.configureBlocking(false);
        this.selector = Selector.open();
        this.server.register(selector, SelectionKey.OP_ACCEPT);
        this.name = chronicle.name() + "@" + address.getPort();
        this.logger = LoggerFactory.getLogger(getClass().getName() + "." + name);
        this.service = Executors.newCachedThreadPool(new NamedThreadFactory(name, true));
        this.service.execute(new Acceptor());
        this.maxMessages = config.maxMessages();
    }

    @Override
    public String name() {
        return chronicle.name();
    }

    public int getLocalPort() {
        return server.socket().getLocalPort();
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
            server.close();
            service.shutdownNow();
            service.awaitTermination(10, TimeUnit.SECONDS);

            chronicle.close();
        } catch (IOException e) {
            logger.warn("Error closing server port", e);
        } catch (InterruptedException ie) {
            logger.warn("Error shutting down service threads", ie);
        }
    }

    @Override
    public Excerpt createExcerpt() throws IOException {
        return new SourceExcerpt(chronicle.createExcerpt());
    }

    @Override
    public ExcerptTailer createTailer() throws IOException {
        return new SourceExcerpt(chronicle.createTailer());
    }

    @Override
    public ExcerptAppender createAppender() throws IOException {
        return new SourceExcerpt(chronicle.createAppender());
    }

    protected void checkCounts(int min, int max) {
        if(chronicle instanceof VanillaChronicle) {
            ((VanillaChronicle)chronicle).checkCounts(min, max);
        }
    }

    protected void pauseReset() {
        lastUnpausedNS = System.nanoTime();
    }

    protected void pause() {
        if (lastUnpausedNS + busyWaitTimeNS > System.nanoTime())
            return;
        try {
            synchronized (notifier) {
                notifier.wait(config.heartbeatInterval() / 2);
            }
        }
        catch (InterruptedException ie) {
            logger.warn("Interrupt ignored");
        }
    }

    protected void wakeSessionHandlers() {
        synchronized (notifier) {
            notifier.notifyAll();
        }
    }

    protected Runnable createSocketHandler(final SocketChannel channel) throws IOException {
        return (chronicle instanceof IndexedChronicle)
            ? new IndexedSocketHandler(channel)
            : new VanillaSocketHandler(channel);
    }

    protected Chronicle chronicle() {
        return this.chronicle;
    }

    protected ChronicleSourceConfig config() {
        return this.config;
    }

    protected boolean closed() {
        return this.closed;
    }

    // *************************************************************************
    //
    // *************************************************************************

    private final class Acceptor implements Runnable {
        @Override
        public void run() {
            Thread.currentThread().setName(name + "-acceptor");
            try {
                while (!closed) {
                    selector.select();

                    final Set<SelectionKey> keys = selector.keys();
                    for (final SelectionKey key : keys) {
                        if (key.isAcceptable()) {
                            final SocketChannel socket = server.accept();
                            socket.configureBlocking(true);
                            logger.info("Accepted connection from: " + socket.getRemoteAddress());
                            service.execute(createSocketHandler(socket));
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


    private final class SourceExcerpt extends WrappedExcerpt {
        public SourceExcerpt(final ExcerptCommon excerptCommon) {
            super(excerptCommon);
        }

        @Override
        public void finish() {
            super.finish();
            wakeSessionHandlers();
        }
    }

    // *************************************************************************
    //
    // *************************************************************************

    private final class IndexedSocketHandler extends ChronicleSourceSocketHandler {
        private long index;

        public IndexedSocketHandler(@NotNull SocketChannel socket) throws IOException {
            super(ChronicleSource.this, socket, logger);
            this.index = -1;
        }

        @Override
        protected boolean handleSubscribe(final SelectionKey key) throws IOException {
            this.index = command.data();
            if (this.index == -1) {
                this.index = -1;
            } else if (this.index == -2) {
                this.index = tailer.toEnd().index();
            }


            sendSizeAndIndex(ChronicleTcp.SYNC_IDX_LEN, this.index);

            key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
            return false;
        }

        @Override
        protected boolean publishData() throws IOException {
            logger.info("publishData {}", index);
            if (!tailer.index(index)) {
                if (tailer.wasPadding()) {
                    if (index >= 0) {
                        sendSizeAndIndex(ChronicleTcp.PADDED_LEN, tailer.index());
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
            long remaining = size + ChronicleTcp.HEADER_SIZE;

            buffer.clear();
            buffer.putInt((int) size);
            buffer.putLong(tailer.index());
            logger.info("Sending {}", tailer.index());

            // for large objects send one at a time.
            if (size > buffer.capacity() / 2) {
                while (remaining > 0) {
                    int size2 = (int) Math.min(remaining, buffer.capacity());
                    buffer.limit(size2);
                    tailer.read(buffer);
                    buffer.flip();
                    remaining -= buffer.remaining();
                    ChronicleTcp.writeAll(socket, buffer);
                }
            } else {
                buffer.limit((int) remaining);
                tailer.read(buffer);
                int count = 1;
                while (tailer.index(index + 1) && count++ < maxMessages) {
                    if(!tailer.wasPadding()) {
                        if (tailer.capacity() + ChronicleTcp.HEADER_SIZE >= (buffer.capacity() - buffer.position())) {
                            break;
                        }

                        // if there is free space, copy another one.
                        int size2 = (int) tailer.capacity();
                        buffer.limit(buffer.position() + size2 + ChronicleTcp.HEADER_SIZE);
                        buffer.putInt(size2);
                        buffer.putLong(tailer.index());
                        tailer.read(buffer);
                        index++;
                    } else {
                        index++;
                    }
                }

                buffer.flip();
                ChronicleTcp.writeAll(socket, buffer);
            }

            if (buffer.remaining() > 0) {
                throw new EOFException("Failed to send index=" + index);
            }

            index++;
            return true;
        }
    }

    private final class VanillaSocketHandler extends ChronicleSourceSocketHandler {
        private boolean nextIndex;
        private long index;

        public VanillaSocketHandler(@NotNull SocketChannel socket) throws IOException {
            super(ChronicleSource.this, socket, logger);
            this.nextIndex = true;
            this.index = -1;
        }

        @Override
        protected boolean handleSubscribe(final SelectionKey key) throws IOException {
            this.index = command.data();
            if (this.index == -1) {
                this.nextIndex = true;
                this.tailer = tailer.toStart();
                this.index = -1;
            } else if (this.index == -2) {
                this.nextIndex = false;
                this.tailer = tailer.toEnd();
                this.index = tailer.index();
            } else {
                this.nextIndex = false;
            }

            sendSizeAndIndex(ChronicleTcp.SYNC_IDX_LEN, this.index);

            key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
            return false;
        }

        @Override
        protected boolean publishData() throws IOException {
            if(nextIndex) {
                if (!tailer.nextIndex()) {
                    pause();
                    if (!closed && !tailer.nextIndex()) {
                        return false;
                    }
                }
            } else {
                if(!tailer.index(this.index)) {
                    return false;
                } else {
                    this.nextIndex = true;
                }
            }

            pauseReset();

            final long size = tailer.capacity();
            long remaining = size + ChronicleTcp.HEADER_SIZE;

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
                    ChronicleTcp.writeAll(socket, buffer);
                }
            } else {
                buffer.limit((int) remaining);
                tailer.read(buffer);
                int count = 1;
                while (count++ < maxMessages) {
                    if (tailer.nextIndex()) {
                        if (tailer.wasPadding()) {
                            throw new AssertionError("Entry should not be padding - remove");
                        }

                        if (tailer.capacity() + ChronicleTcp.HEADER_SIZE >= buffer.capacity() - buffer.position()) {
                            break;
                        }

                        // if there is free space, copy another one.
                        int size2 = (int) tailer.capacity();
                        buffer.limit(buffer.position() + size2 + ChronicleTcp.HEADER_SIZE);
                        buffer.putInt(size2);
                        buffer.putLong(tailer.index());
                        tailer.read(buffer);
                    }
                }

                buffer.flip();
                ChronicleTcp.writeAll(socket, buffer);
            }

            if (buffer.remaining() > 0) {
                throw new EOFException("Failed to send index=" + tailer.index());
            }

            return true;
        }
    }
}
