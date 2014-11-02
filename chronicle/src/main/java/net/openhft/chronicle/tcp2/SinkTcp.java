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

import net.openhft.chronicle.ChronicleQueueBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;


public abstract class SinkTcp {
    protected final Logger logger;
    protected final String name;
    protected final AtomicBoolean running;
    protected final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder;

    private SocketChannel socketChannel;

    protected SinkTcp(String name, final ChronicleQueueBuilder.ReplicaChronicleQueueBuilder builder) {
        this.builder = builder;
        this.name = ChronicleTcp2.connectionName(name, this.builder.bindAddress(), this.builder.connectAddress());
        this.logger = LoggerFactory.getLogger(this.name);
        this.running = new AtomicBoolean(false);
    }

    @Override
    public String toString() {
        return this.name;
    }

    public boolean open() throws IOException {
        close();
        running.set(true);

        socketChannel = openSocketChannel();
        if(socketChannel != null) {
            socketChannel.configureBlocking(false);
            socketChannel.socket().setTcpNoDelay(true);
            socketChannel.socket().setReceiveBufferSize(this.builder.receiveBufferSize());
            socketChannel.socket().setSoTimeout(0);
            socketChannel.socket().setSoLinger(false, 0);
        }

        running.set(false);

        return socketChannel != null;
    }

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

    public boolean read(ByteBuffer buffer) throws IOException {
        if (this.socketChannel.read(buffer) < 0) {
            throw new EOFException();
        }

        return true;
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

    public abstract boolean isLocalhost();
    protected abstract SocketChannel openSocketChannel() throws IOException;
}
