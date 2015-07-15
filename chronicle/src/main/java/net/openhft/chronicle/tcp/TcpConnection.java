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

import net.openhft.lang.io.Bytes;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

class TcpConnection {

    private SocketChannel socketChannel;

    private ByteBuffer buffer = ByteBuffer.allocate(1);

    public TcpConnection() {
        this(null);
    }

    public TcpConnection(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
    }

    protected void setSocketChannel(SocketChannel socketChannel) throws IOException {
        if (this.socketChannel != null) {
            close();
        }

        this.socketChannel = socketChannel;
    }

    public SocketChannel socketChannel() {
        return this.socketChannel;
    }

    public boolean isOpen() {
        if (this.socketChannel != null) {
            return this.socketChannel.isOpen();
        }

        return false;
    }

    public void close() throws IOException {
        if (socketChannel != null) {
            if (socketChannel.isOpen()) {
                socketChannel.close();
            }

            socketChannel = null;
        }
    }

    public String debugString() {
        if (this.socketChannel != null) {
            try {
                StringBuilder sb = new StringBuilder();
                sb.append("[");
                sb.append(this.socketChannel.getLocalAddress());
                sb.append(" -> ");
                sb.append(this.socketChannel.getRemoteAddress());
                sb.append("]");
            } catch (IOException e) {
            }
        }

        return "[] -> []";
    }

    public int write(final Bytes bytes) throws IOException {
        final ByteBuffer bb = bytes.sliceAsByteBuffer(buffer);
        int bw = write0(bb);
        bytes.position(bb.position());
        return bw;
    }

    private int write0(final ByteBuffer bb) throws IOException {
        int bw = 0;
        while (bb.remaining() > 0) {
            bw = this.socketChannel.write(bb);
            if (bw < 0) {
                break;
            }
        }
        return bw;
    }

    public boolean read(final Bytes bytes, int size, int readAttempts) throws IOException {
        final ByteBuffer buffer = bytes.sliceAsByteBuffer(this.buffer);
        boolean result = read0(buffer, size, readAttempts);
        bytes.limit(buffer.limit() + bytes.position()).position(0); // bytes.flip();
        return result;
    }

    private boolean read0(ByteBuffer buffer, int toRead, int readAttempts) throws IOException {
        int spins = 0;
        int bytesRead = 0;
        while (bytesRead < toRead) {
            int rb = this.socketChannel.read(buffer);
            if (rb < 0) {
                throw new EOFException();
            } else if (bytesRead == 0 && rb == 0 && readAttempts > -1) {
                if (spins++ >= readAttempts) {
                    buffer.flip();
                    // this can only return false when nothing has been read.
                    // is there any point flipping the buffer?
                    return false;
                }
            } else {
                spins = 0;
                bytesRead += rb;
            }
        }
        buffer.flip();
        return true;
    }

    public void writeAction(ByteBuffer buffer, long action, long size) throws IOException {
        buffer.clear();
        buffer.putLong(action);
        buffer.putLong(size);
        buffer.flip();

        write0(buffer);
    }
}
