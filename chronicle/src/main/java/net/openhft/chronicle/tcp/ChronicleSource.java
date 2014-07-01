/*
 * Copyright 2014 Higher Frequency Trading
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

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.Excerpt;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptCommon;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.IndexedChronicle;
import net.openhft.chronicle.VanillaChronicle;
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
import java.util.concurrent.TimeUnit;

public class ChronicleSource implements Chronicle {

    static final int IN_SYNC_LEN = -128;
    static final int PADDED_LEN = -127;
    static final int SYNC_IDX_LEN = -126;
    static final int MAX_MESSAGE = 128;
    static final long HEARTBEAT_INTERVAL_MS = 2500;

    @NotNull
    private final Chronicle chronicle;
    private final ChronicleSourceConfig sourceConfig;
    private final ServerSocketChannel server;
    private final Selector selector;
    @NotNull
    private final String name;
    @NotNull
    private final ExecutorService service;
    private final Logger logger;
    private final Object notifier = new Object();
    private static final long busyWaitTimeNS = 100 * 1000;
    protected volatile boolean closed = false;
    private long lastUnpausedNS = 0;

    public ChronicleSource(@NotNull final Chronicle chronicle, final int port) throws IOException {
        this(chronicle, ChronicleSourceConfig.DEFAULT, new InetSocketAddress(port));
    }

    public ChronicleSource(@NotNull final Chronicle chronicle, @NotNull final InetSocketAddress address) throws IOException {
        this(chronicle, ChronicleSourceConfig.DEFAULT, address);
    }
    public ChronicleSource(@NotNull final Chronicle chronicle, @NotNull final ChronicleSourceConfig sourceConfig, final int port) throws IOException {
        this(chronicle, sourceConfig, new InetSocketAddress(port));
    }

    public ChronicleSource(@NotNull final Chronicle chronicle, @NotNull final ChronicleSourceConfig sourceConfig, @NotNull final InetSocketAddress address) throws IOException {
        this.chronicle = chronicle;
        this.sourceConfig = sourceConfig;
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
            chronicle.close();
            server.close();
            service.shutdownNow();
            service.awaitTermination(10000, TimeUnit.MILLISECONDS);
        }
        catch (IOException e) {
            logger.warn("Error closing server port", e);
        }
        catch (InterruptedException ie) {
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

    public void checkCounts(int min, int max) {
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
                notifier.wait(HEARTBEAT_INTERVAL_MS / 2);
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

    protected long readIndex(@NotNull SocketChannel socket) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(8);
        TcpUtil.readFullyOrEOF(socket, bb);
        return bb.getLong(0);
    }

    protected Runnable createSocketHandler(SocketChannel channel) throws IOException {
        return (chronicle instanceof IndexedChronicle)
            ? new IndexedSocketHandler(channel)
            : new VanillaSocketHandler(channel);
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
                    Set<SelectionKey> keys = selector.keys();
                    for (SelectionKey key : keys) {
                        if (key.isAcceptable()) {
                            SocketChannel socket = server.accept();
                            socket.configureBlocking(true);
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
        public SourceExcerpt(ExcerptCommon excerptCommon) {
            super(excerptCommon);
        }

        @Override
        public void finish() {
            super.finish();
            wakeSessionHandlers();
        }
    }

    protected abstract class AbstractSocketHandler implements Runnable {
        protected final SocketChannel socket;
        protected final Selector selector;
        protected final ByteBuffer buffer;
        protected ExcerptTailer tailer;

        protected long index;
        protected long lastHeartbeatTime;

        public AbstractSocketHandler(@NotNull SocketChannel socket) throws IOException {
            this.socket = socket;
            this.socket.configureBlocking(false);
            this.socket.socket().setSendBufferSize(sourceConfig.minBufferSize());
            this.socket.socket().setTcpNoDelay(true);
            this.selector = Selector.open();
            this.tailer = chronicle.createTailer();
            this.buffer = TcpUtil.createBuffer(1, ByteOrder.nativeOrder());
            this.index = -1;
            this.lastHeartbeatTime = 0;
        }

        @Override
        public void run() {
            try {
                socket.register(selector, SelectionKey.OP_READ);

                while(!closed) {
                    if (selector.select() > 0) {
                        final Set<SelectionKey> keys = selector.selectedKeys();
                        onSelectResult(keys);
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
                        if(this.socket.isOpen()) {
                            this.socket.close();
                        }
                    } catch(IOException ioe) {
                        logger.warn("",e);
                    }
                }
            }

            if(tailer != null) {
                tailer.close();
            }
        }

        protected void setLastHeartbeatTime() {
            this.lastHeartbeatTime = System.currentTimeMillis() + HEARTBEAT_INTERVAL_MS;
        }

        protected void setLastHeartbeatTime(long from) {
            this.lastHeartbeatTime = from + HEARTBEAT_INTERVAL_MS;
        }

        protected abstract void onSelectResult(final Set<SelectionKey> keys) throws IOException;
    }

    // *************************************************************************
    //
    // *************************************************************************

    private final class IndexedSocketHandler extends AbstractSocketHandler {
        public IndexedSocketHandler(@NotNull SocketChannel socket) throws IOException {
            super(socket);
        }

        @Override
        public void onSelectResult(final Set<SelectionKey> keys) throws IOException {
            for (Iterator<SelectionKey> it = keys.iterator(); it.hasNext();) {
                final SelectionKey key = it.next();
                it.remove();

                if(key.isReadable()) {
                    try {
                        this.index = readIndex(socket);
                        if(this.index == -1) {
                            this.index = -1;
                        } else if(this.index == -2){
                            this.index = tailer.toEnd().index();
                        }

                        buffer.clear();
                        buffer.putInt(SYNC_IDX_LEN);
                        buffer.putLong(this.index);
                        buffer.flip();
                        TcpUtil.writeAll(socket, buffer);

                        setLastHeartbeatTime();

                        key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                        keys.clear();
                        return;
                    } catch(EOFException e) {
                        key.selector().close();
                        throw e;
                    }
                } else if(key.isWritable()) {
                    final long now = System.currentTimeMillis();
                    if(!closed && !publishData()) {
                        if (lastHeartbeatTime <= now) {
                            buffer.clear();
                            buffer.putInt(IN_SYNC_LEN);
                            buffer.putLong(0L);
                            buffer.flip();
                            TcpUtil.writeAll(socket, buffer);
                            setLastHeartbeatTime(now);
                        }
                    }
                }
            }
        }

        private boolean publishData() throws IOException {
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
    }

    private final class VanillaSocketHandler extends AbstractSocketHandler {
        private boolean nextIndex;

        public VanillaSocketHandler(@NotNull SocketChannel socket) throws IOException {
            super(socket);
            this.nextIndex = true;
        }

        @Override
        public void onSelectResult(final Set<SelectionKey> keys) throws IOException {
            final Iterator<SelectionKey> it = keys.iterator();

            while (it.hasNext()) {
                final SelectionKey key = it.next();
                it.remove();

                if(key.isReadable()) {
                    try {
                        this.index = readIndex(socket);
                        if(this.index == -1) {
                            this.nextIndex = true;
                            this.tailer = tailer.toStart();
                            this.index = -1;
                        } else if(this.index == -2) {
                            this.nextIndex = false;
                            this.tailer = tailer.toEnd();
                            this.index = tailer.index();
                        } else {
                            this.nextIndex = false;
                        }

                        buffer.clear();
                        buffer.putInt(SYNC_IDX_LEN);
                        buffer.putLong(this.index);
                        buffer.flip();
                        TcpUtil.writeAll(socket, buffer);

                        setLastHeartbeatTime();

                        key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                        keys.clear();
                        break;
                    } catch(EOFException e) {
                        key.selector().close();
                        throw e;
                    }
                } else if(key.isWritable()) {
                    final long now = System.currentTimeMillis();
                    if(!closed && !publishData()) {
                        if (lastHeartbeatTime <= now) {
                            buffer.clear();
                            buffer.putInt(IN_SYNC_LEN);
                            buffer.putLong(0L);
                            buffer.flip();
                            TcpUtil.writeAll(socket, buffer);
                            setLastHeartbeatTime(now);
                        }
                    }
                }
            }
        }

        private boolean publishData() throws IOException {
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
                while (count++ < MAX_MESSAGE) {
                    if (tailer.nextIndex()) {
                        if (tailer.wasPadding()) {
                            throw new AssertionError("Entry should not be padding - remove");
                        }

                        if (tailer.capacity() + TcpUtil.HEADER_SIZE >= buffer.capacity() - buffer.position()) {
                            break;
                        }

                        // if there is free space, copy another one.
                        int size2 = (int) tailer.capacity();
                        buffer.limit(buffer.position() + size2 + TcpUtil.HEADER_SIZE);
                        buffer.putInt(size2);
                        buffer.putLong(tailer.index());
                        tailer.read(buffer);
                    }
                }

                buffer.flip();
                TcpUtil.writeAll(socket, buffer);
            }

            if (buffer.remaining() > 0) {
                throw new EOFException("Failed to send index=" + tailer.index());
            }

            return true;
        }
    }
}
