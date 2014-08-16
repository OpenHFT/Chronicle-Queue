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

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.lang.model.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

public abstract class ChronicleSourceSocketHandler implements Runnable, Closeable {
    private final Logger logger;
    private final Chronicle chronicle;
    private final ChronicleSource source;
    private final ChronicleSourceConfig config;

    protected final SocketChannel socket;
    protected final Selector selector;
    protected final ByteBuffer buffer;

    protected ExcerptTailer tailer;
    protected long lastHeartbeat;
    protected ChronicleTcp.Command command;

    protected ChronicleSourceSocketHandler(
        final @NotNull ChronicleSource source,
        final @NotNull SocketChannel socket,
        final @NotNull Logger logger) throws IOException {
        this.logger = logger;
        this.source = source;
        this.chronicle = this.source.chronicle();
        this.config = this.source.config();
        this.tailer = null;
        this.buffer = ChronicleTcp.createBuffer(1, ByteOrder.nativeOrder());
        this.command = new ChronicleTcp.Command();
        this.lastHeartbeat = 0;

        this.socket = socket;
        this.socket.configureBlocking(false);
        this.socket.socket().setSendBufferSize(this.config.minBufferSize());
        this.socket.socket().setTcpNoDelay(true);

        this.selector = Selector.open();
    }

    @Override
    public void close() throws IOException {
        if(this.tailer != null) {
            this.tailer.close();
            this.tailer = null;
        }

        if(this.socket.isOpen()) {
            this.socket.close();
        }
    }

    @Override
    public void run() {
        try {
            tailer = chronicle.createTailer();
            socket.register(selector, SelectionKey.OP_READ);

            while(!this.source.closed() && !Thread.currentThread().isInterrupted()) {
                if (selector.select(this.config.selectTimeout()) > 0) {
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
            if (!this.source.closed()) {
                logger.info("Connection {} died",socket);
            }
        } catch (Exception e) {
            if (!this.source.closed()) {
                String msg = e.getMessage();
                if (msg != null &&
                    (msg.contains("reset by peer")
                        || msg.contains("Broken pipe")
                        || msg.contains("was aborted by"))) {
                    logger.info("Connection {} closed from the other end: ", socket, e.getMessage());
                } else {
                    logger.info("Connection {} died",socket, e);
                }
            }
        }

        try {
            close();
        } catch(IOException e) {
            logger.warn("",e);
        }
    }

    protected void setLastHeartbeat() {
        this.lastHeartbeat = System.currentTimeMillis() + this.config.heartbeatInterval();
    }

    protected void setLastHeartbeat(long from) {
        this.lastHeartbeat = from + this.config.heartbeatInterval();
    }

    protected void sendSizeAndIndex(int size, long index) throws IOException {
        buffer.clear();
        buffer.putInt(size);
        buffer.putLong(index);
        buffer.flip();
        ChronicleTcp.writeAll(socket, buffer);
        setLastHeartbeat();
    }

    protected boolean handleSubscribe(final SelectionKey key) throws IOException {
        return true;
    }

    protected boolean handleQuery(final SelectionKey key) throws IOException {
        if(tailer.index(command.data())) {
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

    protected boolean onRead(final SelectionKey key) throws IOException {
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
    }

    protected boolean onWrite(final SelectionKey key) throws IOException {
        final long now = System.currentTimeMillis();
        if(!this.source.closed() && !publishData()) {
            if (lastHeartbeat <= now) {
                sendSizeAndIndex(ChronicleTcp.IN_SYNC_LEN, 0L);
            }
        }

        return true;
    }

    protected abstract boolean publishData() throws IOException;
}
