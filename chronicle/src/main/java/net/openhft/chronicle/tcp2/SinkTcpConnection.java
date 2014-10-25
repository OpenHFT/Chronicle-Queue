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
package net.openhft.chronicle.tcp2;

import net.openhft.chronicle.tcp.ChronicleSinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


public abstract class SinkTcpConnection implements TcpConnection {
    protected final Logger logger;
    protected final String name;
    protected final InetSocketAddress bindAddress;
    protected final InetSocketAddress connectAddress;
    protected final AtomicBoolean running;

    protected long reconnectTimeout;
    protected TimeUnit reconnectTimeoutUnit;
    protected long selectTimeout;
    protected TimeUnit selectTimeoutUnit;
    protected int maxOpenAttempts;
    protected int receiveBufferSize;
    protected boolean tcpNoDelay;

    private SocketChannel socketChannel;

    protected SinkTcpConnection(String name, final InetSocketAddress bindAddress, final InetSocketAddress connectAddress) {
        this.name = ChronicleTcp2.connectionName(name, bindAddress, connectAddress);
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

    @Override
    public String toString() {
        return this.name;
    }

    @Override
    public boolean open() throws IOException {
        close();
        running.set(true);

        socketChannel = openSocketChannel();
        if(socketChannel != null) {
            socketChannel.configureBlocking(false);
            socketChannel.socket().setTcpNoDelay(this.tcpNoDelay);
            socketChannel.socket().setReceiveBufferSize(this.receiveBufferSize);
            socketChannel.socket().setSoTimeout(0);
            socketChannel.socket().setSoLinger(false, 0);
        }

        running.set(false);

        return socketChannel != null;
    }

    @Override
    public boolean close()  throws IOException {
        this.running.set(false);

        if(socketChannel != null) {
            if(socketChannel.isOpen()) {
                socketChannel.close();
            }
        }

        socketChannel = null;

        return true;
    }

    @Override
    public boolean isOpen() {
        if(this.socketChannel != null) {
            return this.socketChannel.isOpen();
        }

        return false;
    }

    @Override
    public boolean write(ByteBuffer buffer) throws IOException {
        return true;
    }

    @Override
    public void writeAllOrEOF(ByteBuffer bb) throws IOException {
        writeAll(bb);

        if (bb.remaining() > 0) {
            throw new EOFException();
        }
    }

    @Override
    public void writeAll(ByteBuffer bb) throws IOException {
        while (bb.remaining() > 0) {
            if (this.socketChannel.write(bb) < 0) {
                break;
            }
        }
    }

    @Override
    public boolean read(ByteBuffer buffer, int size) throws IOException {
        return read(buffer, size, size);
    }

    @Override
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

    protected abstract SocketChannel openSocketChannel() throws IOException;
}
