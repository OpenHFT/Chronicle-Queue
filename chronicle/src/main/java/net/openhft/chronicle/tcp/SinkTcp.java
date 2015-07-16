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
import net.openhft.chronicle.Excerpt;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.network.*;
import net.openhft.lang.thread.LightPauser;
import net.openhft.lang.thread.Pauser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static net.openhft.chronicle.network.TcpPipeline.pipeline;

public abstract class SinkTcp {// extends TcpConnection {
    protected final Logger logger;
    protected final String name;
    protected final AtomicBoolean running;
    protected final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder;
    protected final SessionDetailsProvider sessionDetailsProvider = new SimpleSessionDetailsProvider();
    protected TcpEventHandler tcpEventHandler;
    protected SocketChannel socketChannel;

    private long reconnectionIntervalMS;
    private long lastReconnectionAttempt;
    private long lastReconnectionAttemptMS;
    private ConnectionListener connectionListener;
    private TcpHandler sinkTcpHandler;

    protected SinkTcp(String name, final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder) {
        this.builder = builder;
        this.name = ChronicleTcp.connectionName(name, this.builder.bindAddress(), this.builder.connectAddress());
        this.logger = LoggerFactory.getLogger(this.name);
        this.running = new AtomicBoolean(false);
        this.reconnectionIntervalMS = builder.reconnectionIntervalMillis();
    }

    public void setSinkTcpHandler(TcpHandler sinkTcpHandler) {
        this.sinkTcpHandler = sinkTcpHandler;
    }

    @Override
    public String toString() {
        return this.name;
    }

    public boolean open() throws IOException {
        return open(false);
    }

    public boolean open(boolean blocking) throws IOException {
        close();
        running.set(true);

        SocketChannel socketChannel = openSocketChannel();
        if(socketChannel != null) {

            socketChannel.configureBlocking(blocking);
            socketChannel.socket().setTcpNoDelay(true);
            socketChannel.socket().setSoTimeout(0);
            socketChannel.socket().setSoLinger(false, 0);

            if(this.builder.receiveBufferSize() > 0) {
                socketChannel.socket().setReceiveBufferSize(this.builder.receiveBufferSize());
            }
            if(this.builder.sendBufferSize() > 0) {
                socketChannel.socket().setSendBufferSize(this.builder.sendBufferSize());
            }

            this.socketChannel = socketChannel;
            this.tcpEventHandler = new TcpEventHandler(socketChannel, pipeline(sinkTcpHandler), sessionDetailsProvider);
        }

        running.set(false);

        return socketChannel != null;
    }

    public void close() throws IOException {
        this.running.set(false);
        if (socketChannel != null) {
            socketChannel.close();
        }
    }

    public boolean isOpen() {
        return socketChannel != null && socketChannel.isOpen();
    }

    protected boolean shouldConnect() {
        if(lastReconnectionAttempt >= builder.reconnectionAttempts()) {
            long now = System.currentTimeMillis();
            if (now < lastReconnectionAttemptMS + reconnectionIntervalMS) {
                return false;
            }

            lastReconnectionAttemptMS = now;
        }

        return true;
    }

    public boolean connect(boolean blocking) {
        if (isOpen()) {
            return true;
        } else if (!shouldConnect()) {
            return false;
        } else if (!isOpen()) {
            try {
                open(blocking);
            } catch (IOException e) {
            }
        }

        boolean connected = isOpen();
        if(connected) {
            this.lastReconnectionAttempt = 0;
            this.lastReconnectionAttemptMS = 0;
            if (connectionListener != null) {
                connectionListener.onConnect();
            }
        } else {
            lastReconnectionAttempt++;
            if(builder.reconnectionWarningThreshold() > 0) {
                if (lastReconnectionAttempt > builder.reconnectionWarningThreshold()) {
                    logger.warn("Failed to establish a connection {}",
                            ChronicleTcp.connectionName("", builder)
                    );
                }
            }
        }

        return connected;
    }

    public void sink() throws IOException {
        try {
            tcpEventHandler.action();
        } catch (InvalidEventHandlerException e) {
            throw new IOException("Failed to sink to remote chronicle.", e);
        }
    }

    public abstract boolean isLocalhost();

    protected abstract SocketChannel openSocketChannel() throws IOException;

    public void setConnectionListener(ConnectionListener connectionListener) {
        this.connectionListener = connectionListener;
    }

    public SocketChannel socketChannel() {
        return socketChannel;
    }

    public interface ConnectionListener {
        void onConnect();
    }
}
