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

import net.openhft.lang.model.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ChronicleSink2Support {

    // *************************************************************************
    //
    // *************************************************************************

    /*
     * TODO: review boolean return for open/close operations
     */
    public static class TcpSink {
        protected final Logger logger;
        protected final String name;
        protected InetSocketAddress bindAddress;
        protected final InetSocketAddress connectAddress;
        protected final AtomicBoolean running;
        protected long reconnectTimeout;
        protected TimeUnit reconnectTimeoutUnit;
        protected long selectTimeout;
        protected TimeUnit selectTimeoutUnit;
        protected SocketChannel socket;
        protected int maxOpenAttempts;

        public TcpSink(String name, final InetSocketAddress connectAddress) {
            this(name, null, connectAddress);
        }

        public TcpSink(String name, final InetSocketAddress bindAddress, final InetSocketAddress connectAddress) {
            this.name = name;
            this.logger = LoggerFactory.getLogger(this.name);
            this.bindAddress = bindAddress;
            this.connectAddress = connectAddress;
            this.running = new AtomicBoolean(false);
            this.reconnectTimeout = 500;
            this.reconnectTimeoutUnit = TimeUnit.MILLISECONDS;
            this.selectTimeout = 1000;
            this.selectTimeoutUnit = TimeUnit.MILLISECONDS;
            this.socket = null;
            this.maxOpenAttempts = Integer.MAX_VALUE;
        }

        public boolean open() throws IOException {
            return false;
        }

        public boolean close()  throws IOException {
            this.running.set(false);

            if(socket != null) {
                socket.close();
            }

            return true;
        }

        public void setBindAddress(final @NotNull InetSocketAddress bindAddress) {
            this.bindAddress = bindAddress;
        }

        public void setReconnectTimeout(long reconnectTimeout, TimeUnit reconnectTimeoutUnit) {
            if(reconnectTimeout < 0) {
                throw new IllegalArgumentException("ReconnectTimeout must be >= 0");
            }

            this.reconnectTimeout = reconnectTimeout;
            this.reconnectTimeoutUnit = reconnectTimeoutUnit;
        }

        public void setSelectTimeout(long selectTimeout, TimeUnit selectTimeoutUnit) {
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

        public boolean isOpen() {
            if(this.socket != null) {
                return this.socket.isOpen();
            }

            return false;
        }

        public SocketChannel channel() {
            return this.socket;
        }
    }

    public static class TcpSinkInitiator extends TcpSink {
        public TcpSinkInitiator(String name, final InetSocketAddress connectAddress) {
            super(name + "-sink-initiator", connectAddress);
        }

        @Override
        public boolean open() throws IOException {
            running.set(true);
            socket = null;

            final Selector selector = Selector.open();

            final SocketChannel channel = SocketChannel.open();
            channel.configureBlocking(false);

            if(bindAddress != null) {
                channel.bind(bindAddress);
            }

            channel.register(selector, SelectionKey.OP_CONNECT);
            channel.connect(connectAddress);

            for (int i=0; i< maxOpenAttempts && this.running.get() && socket == null; i++) {
                if(selector.select(selectTimeoutUnit.toMillis(selectTimeout)) > 0) {
                    final Set<SelectionKey> keys = selector.keys();
                    for (final SelectionKey key : keys) {
                        if (key.isConnectable()) {
                            logger.info("Connected to " + socket.getRemoteAddress() + " from " + socket.getLocalAddress());
                            socket = channel;
                            break;
                        }
                    }

                    keys.clear();
                } else {
                    logger.info("Failed to connect to {}, retrying", connectAddress);
                    socket = null;
                }
            }

            if(socket != null) {
                running.set(false);
                return false;
            }

            return true;
        }
    }

    public static class TcpSinkAcceptor extends TcpSink {
        public TcpSinkAcceptor(String name, final InetSocketAddress bindAddress) {
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
                    final Set<SelectionKey> keys = selector.keys();
                    for (final SelectionKey key : keys) {
                        if (key.isAcceptable()) {
                            socket = server.accept();
                            logger.info("Accepted connection from: " + socket.getRemoteAddress());

                            break;
                        }
                    }

                    keys.clear();
                } else {
                    logger.info("No incoming gonnections to {}, wait", connectAddress);
                    socket = null;
                }
            }

            selector.close();
            server.close();

            if(socket != null) {
                running.set(false);
                return false;
            }

            return true;
        }
    }

    // *************************************************************************
    //
    // *************************************************************************

    public static class PersistedSinkExcerpt {
    }

    public static class VolatileSinkExcerpt {
    }
}
