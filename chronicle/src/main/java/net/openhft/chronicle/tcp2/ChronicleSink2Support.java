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
package net.openhft.chronicle.tcp2;

import net.openhft.chronicle.tcp.ChronicleSinkConfig;
import net.openhft.lang.model.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ChronicleSink2Support {

    /*
     * TODO: review boolean return for open/close operations
     */
    public static class TcpConnection {
        protected final Logger logger;
        protected final String name;
        protected InetSocketAddress bindAddress;
        protected final InetSocketAddress connectAddress;
        protected final AtomicBoolean running;
        protected long reconnectTimeout;
        protected TimeUnit reconnectTimeoutUnit;
        protected long selectTimeout;
        protected TimeUnit selectTimeoutUnit;
        protected SocketChannel socketChannel;
        protected int maxOpenAttempts;
        protected int receiveBufferSize;
        protected boolean tcpNoDelay;

        public TcpConnection(String name, final InetSocketAddress connectAddress) {
            this(name, null, connectAddress);
        }

        public TcpConnection(String name, final InetSocketAddress bindAddress, final InetSocketAddress connectAddress) {
            this.name = name;
            this.logger = LoggerFactory.getLogger(this.name);
            this.bindAddress = bindAddress;
            this.connectAddress = connectAddress;
            this.running = new AtomicBoolean(false);
            this.reconnectTimeout = 500;
            this.reconnectTimeoutUnit = TimeUnit.MILLISECONDS;
            this.selectTimeout = 1000;
            this.selectTimeoutUnit = TimeUnit.MILLISECONDS;
            this.socketChannel = null;
            this.maxOpenAttempts = Integer.MAX_VALUE;
            this.receiveBufferSize = ChronicleSinkConfig.DEFAULT.minBufferSize();
            this.tcpNoDelay = true;
        }

        public String name() {
            return this.name;
        }

        public boolean open() throws IOException {
            return false;
        }

        public boolean close()  throws IOException {
            this.running.set(false);

            if(socketChannel != null) {
                socketChannel.close();
            }

            return true;
        }

        public void bindAddress(final @NotNull InetSocketAddress bindAddress) {
            this.bindAddress = bindAddress;
        }

        public void reconnectTimeout(long reconnectTimeout, TimeUnit reconnectTimeoutUnit) {
            if(reconnectTimeout < 0) {
                throw new IllegalArgumentException("ReconnectTimeout must be >= 0");
            }

            this.reconnectTimeout = reconnectTimeout;
            this.reconnectTimeoutUnit = reconnectTimeoutUnit;
        }

        public void selectTimeout(long selectTimeout, TimeUnit selectTimeoutUnit) {
            if(selectTimeout < 0) {
                throw new IllegalArgumentException("SelectTimeout must be >= 0");
            }

            this.selectTimeout = selectTimeout;
            this.selectTimeoutUnit = selectTimeoutUnit;
        }

        public void maxOpenAttempts(int maxOpenAttempts) {
            if(maxOpenAttempts <= 0) {
                throw new IllegalArgumentException("MaxOpenAttempts must be > 0");
            }

            this.maxOpenAttempts = maxOpenAttempts;
        }

        public void receiveBufferSize(int receiveBufferSize) {
            if(receiveBufferSize <= 0) {
                throw new IllegalArgumentException("ReceiveBufferSize must be > 0");
            }

            this.receiveBufferSize = receiveBufferSize;
        }

        public void tcpNoDelay(boolean tcpNoDelay) {
            this.tcpNoDelay = tcpNoDelay;
        }

        public boolean isOpen() {
            if(this.socketChannel != null) {
                return this.socketChannel.isOpen();
            }

            return false;
        }

        public boolean write(ByteBuffer buffer) throws IOException {
            return true;
        }

        public void writeAllOrEOF(ByteBuffer bb) throws IOException {
            writeAll(bb);

            if (bb.remaining() > 0) {
                throw new EOFException();
            }
        }

        public void writeAll(ByteBuffer bb) throws IOException {
            while (bb.remaining() > 0) {
                if (this.socketChannel.write(bb) < 0) {
                    break;
                }
            }
        }

        public boolean read(ByteBuffer buffer, int size) throws IOException {
            return read(buffer, size, size);
        }

        public boolean read(ByteBuffer buffer, int threshod, int size) throws IOException {
            int rem = buffer.remaining();
            if (rem < threshod) {
                if (buffer.remaining() == 0) {
                    buffer.clear();
                } else {
                    buffer.compact();
                }

                while (buffer.position() < size) {
                    if (this.socketChannel.read(buffer) < 0) {
                        this.socketChannel.close();
                        return false;
                    }
                }

                buffer.flip();
            }

            return true;
        }
    }

    public static class TcpConnectionInitiator extends TcpConnection {
        public TcpConnectionInitiator(String name, final InetSocketAddress connectAddress) {
            super(name + "-sink-initiator", connectAddress);
        }

        @Override
        public boolean open() throws IOException {
            running.set(true);
            socketChannel = null;

            for (int i = 0; i < maxOpenAttempts && this.running.get() && socketChannel == null; i++) {
                try {
                    final SocketChannel channel = SocketChannel.open();
                    channel.configureBlocking(true);

                    if(bindAddress != null) {
                        channel.bind(bindAddress);
                    }

                    channel.connect(connectAddress);

                    socketChannel = channel;
                    logger.info("Connected to " + socketChannel.getRemoteAddress() + " from " + socketChannel.getLocalAddress());
                } catch(IOException e) {
                    socketChannel = null;
                    logger.info("Failed to connect to {}, retrying", connectAddress);

                    try {
                        Thread.sleep(reconnectTimeoutUnit.toMillis(reconnectTimeout));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }

            if(socketChannel != null) {
                socketChannel.configureBlocking(false);
                socketChannel.socket().setTcpNoDelay(this.tcpNoDelay);
                socketChannel.socket().setReceiveBufferSize(this.receiveBufferSize);
                running.set(false);
                return false;
            }

            return true;
        }
    }

    public static class TcpConnectionAcceptor extends TcpConnection {
        public TcpConnectionAcceptor(String name, final InetSocketAddress bindAddress) {
            super(name + "-sink-acceptor", bindAddress);
        }

        @Override
        public boolean open() throws IOException {
            running.set(true);

            final Selector selector = Selector.open();

            final ServerSocketChannel server = ServerSocketChannel.open();
            server.socket().setReuseAddress(true);
            server.socket().bind(this.bindAddress);
            server.configureBlocking(false);

            server.register(selector, SelectionKey.OP_ACCEPT);
            for (int i=0; i< maxOpenAttempts && this.running.get(); i++) {
                if(selector.select(selectTimeoutUnit.toMillis(selectTimeout)) > 0) {
                    final Set<SelectionKey> keys = selector.selectedKeys();
                    for (final SelectionKey key : keys) {
                        if (key.isAcceptable()) {
                            socketChannel = server.accept();
                            logger.info("Accepted connection from: " + socketChannel.getRemoteAddress());

                            break;
                        } else {
                            socketChannel = null;
                        }
                    }

                    keys.clear();
                } else {
                    logger.info("No incoming gonnections to {}, wait", connectAddress);
                    socketChannel = null;
                }
            }

            selector.close();
            server.close();

            if(socketChannel != null) {
                socketChannel.socket().setTcpNoDelay(this.tcpNoDelay);
                socketChannel.socket().setReceiveBufferSize(this.receiveBufferSize);
                running.set(false);
                return false;
            }

            return true;
        }
    }
}
