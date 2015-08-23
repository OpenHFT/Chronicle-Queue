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

import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.tcp.network.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class SinkTcp {
    protected final Logger logger;
    protected final String name;
    protected final AtomicBoolean running;
    protected final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder;
    protected final SessionDetailsProvider sessionDetailsProvider = new SimpleSessionDetailsProvider();
    private final boolean blocking;
    protected SinkTcpEventHandler tcpEventHandler;

    protected SocketChannel socketChannel;
    private long reconnectionIntervalMS;
    private long lastReconnectionAttempt;
    private long lastReconnectionAttemptMS;
    private TcpHandler sinkTcpHandler;

    private final List<TcpConnectionListener> connectionListeners = new ArrayList<>();

    protected SinkTcp(String name, final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder, boolean blocking) {
        this.builder = builder;
        this.name = ChronicleTcp.connectionName(name, this.builder.bindAddress(), this.builder.connectAddress());
        this.blocking = blocking;
        this.logger = LoggerFactory.getLogger(this.name);
        this.running = new AtomicBoolean(false);
        this.reconnectionIntervalMS = builder.reconnectionIntervalMillis();
        this.connectionListeners.add(builder.connectionListener());
    }

    public void setSinkTcpHandler(TcpHandler sinkTcpHandler) {
        this.sinkTcpHandler = sinkTcpHandler;
    }

    @Override
    public String toString() {
        return this.name;
    }

    private boolean open() throws IOException {
        close();
        running.set(true);

        SocketChannel socketChannel = openSocketChannel();
        if (socketChannel != null) {
            this.socketChannel = socketChannel;
            this.tcpEventHandler = new SinkTcpEventHandler(socketChannel, builder.tcpPipeline(sinkTcpHandler), sessionDetailsProvider, builder.sendBufferSize(), builder.receiveBufferSize());
        }

        running.set(false);

        return socketChannel != null;
    }

    public void close() throws IOException {
        this.running.set(false);
        if (socketChannel != null) {
            socketChannel.close();
            try {
                // this will cause the TcpHandlers to close following the closure of the socket.
                tcpEventHandler.action();
            } catch (InvalidEventHandlerException iehe) {
                // expected to be thrown
            }
        }
    }

    public boolean isOpen() {
        return socketChannel != null && socketChannel.isOpen();
    }

    protected boolean shouldConnect() {
        if (lastReconnectionAttempt >= builder.reconnectionAttempts()) {
            long now = System.currentTimeMillis();
            if (now < lastReconnectionAttemptMS + reconnectionIntervalMS) {
                return false;
            }

            lastReconnectionAttemptMS = now;
        }

        return true;
    }

    public boolean connect() {
        if (isOpen()) {
            return true;
        } else if (!shouldConnect()) {
            return false;
        } else if (!isOpen()) {
            try {
                open();
            } catch (IOException e) {
            }
        }

        boolean connected = isOpen();
        if (connected) {
            this.lastReconnectionAttempt = 0;
            this.lastReconnectionAttemptMS = 0;
            for (int i = 0; i < connectionListeners.size(); i++) {
                connectionListeners.get(i).onConnect(socketChannel);
            }
        } else {
            lastReconnectionAttempt++;
            if (builder.reconnectionWarningThreshold() > 0) {
                if (lastReconnectionAttempt > builder.reconnectionWarningThreshold()) {
                    logger.warn("Failed to establish a connection {}",
                            ChronicleTcp.connectionName("", builder)
                    );
                }
            }
        }

        return connected;
    }

    public boolean read() throws IOException {
        return tcpEventHandler.read();
    }

    public boolean write() throws IOException {
        return tcpEventHandler.write();
    }

    public abstract boolean isLocalhost();

    protected abstract SocketChannel openSocketChannel() throws IOException;

    public void addConnectionListener(TcpConnectionListener connectionListener) {
        this.connectionListeners.add(connectionListener);
    }

    public SocketChannel socketChannel() {
        return socketChannel;
    }

    private class SinkTcpEventHandler extends TcpEventHandler {

        private boolean readWasBusy;

        public SinkTcpEventHandler(@NotNull SocketChannel sc, TcpHandler handler, SessionDetailsProvider sessionDetails, int sendCapacity, int receiveCapacity) throws IOException {
            super(sc, handler, sessionDetails, builder.connectionListener(), sendCapacity, receiveCapacity);
            sc.configureBlocking(blocking);
        }

        public boolean write() throws IOException {
            try {
                if (blocking) {
                    return invokeHandler();
                } else {
                    return action();
                }
            } catch (InvalidEventHandlerException e) {
                throw new IOException(e);
            }
        }

        public boolean read() throws IOException {
            try {
                if (blocking && readWasBusy) {
                    // if the last read was busy, try just invoking the handler as we might have data to read
                    // directly from the buffer
                    readWasBusy = invokeHandler();
                }

                if (!blocking || !readWasBusy) {
                    // try a full read, which involves reading from the socket as well
                    readWasBusy = action();
                }

            } catch (InvalidEventHandlerException e) {
                throw new IOException(e);
            }
            return readWasBusy;
        }

    }
}
