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

import net.openhft.lang.model.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

class TcpConnection {

    private SocketChannel socketChannel;

    public TcpConnection() {
        this(null);
    }

    public TcpConnection(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
    }

    protected void setSocketChannel(SocketChannel socketChannel) throws IOException {
        if(this.socketChannel != null) {
            close();
        }

        this.socketChannel = socketChannel;
    }

    public boolean isOpen() {
        if(this.socketChannel != null) {
            return this.socketChannel.isOpen();
        }

        return false;
    }

    public void close() throws IOException {
        if(socketChannel != null) {
            if(socketChannel.isOpen()) {
                socketChannel.close();
            }

            socketChannel = null;
        }
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
            int bw = this.socketChannel.write(bb);
            if (bw < 0) {
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
        Logger l = LoggerFactory.getLogger(this.getClass());
        int rem = buffer.remaining();
        if (rem < threshod) {
            if (buffer.remaining() == 0) {
                buffer.clear();
            } else {
                buffer.compact();
            }

            int targetPosition = buffer.position() + size;
            while (buffer.position() < targetPosition) {
                int rb = this.socketChannel.read(buffer);
                if (rb < 0) {
                    this.socketChannel.close();
                    return false;
                }
            }

            buffer.flip();
        }

        return true;
    }

    public void readFullyOrEOF(@NotNull ByteBuffer bb) throws IOException {
        readAvailable(bb);
        if (bb.remaining() > 0) {
            throw new EOFException();
        }
    }

    public void readAvailable(@NotNull ByteBuffer bb) throws IOException {
        while (bb.remaining() > 0) {
            if (this.socketChannel.read(bb) < 0) {
                break;
            }
        }
    }
}
