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
import net.openhft.lang.io.DirectByteBufferBytes;
import net.openhft.lang.model.constraints.NotNull;

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

    public int write(final ByteBuffer buffer) throws IOException {
        System.out.println("write " + buffer.remaining() + " bytes");
        return this.socketChannel.write(buffer);
    }

    public void writeAllOrEOF(final DirectByteBufferBytes bb) throws IOException {
        writeAllOrEOF(bb.buffer());
    }

    public void writeAllOrEOF(final ByteBuffer bb) throws IOException {
        writeAll(bb);
        if (bb.remaining() > 0) {
            throw new EOFException();
        }
    }

/*
    public void writeAll(final DirectByteBufferBytes bb) throws IOException {
        writeAll(bb.buffer());
    }
*/

    public void writeAll(final ByteBuffer bb) throws IOException {
        int bw = 0;
        System.out.println("write " + bb.remaining() + " bytes");
        while (bb.remaining() > 0) {
            bw = this.socketChannel.write(bb);
            if (bw < 0) {
                break;
            }
        }
    }

/*
    public int read(final ByteBuffer buffer) throws IOException {
        int nb = this.socketChannel.read(buffer);
        if (nb < 0) {
            throw new EOFException();
        }

        return 0;
    }

    public boolean read(final ByteBuffer buffer, int size) throws IOException {
        return read(buffer, size, size);
    }

    public boolean read(final ByteBuffer buffer, int threshod, int size) throws IOException {
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
*/

    public boolean read(final Bytes bytes, int size, int readAttempts) throws IOException {
        return read0(bytes.sliceAsByteBuffer(buffer), size, readAttempts);
    }

    private boolean read0(ByteBuffer buffer, int toRead, int readAttempts) throws IOException {
        int spins = 0;
        int bytesRead = 0;
        System.out.println("br=" + bytesRead + ",tr=" + toRead);
        while (bytesRead < toRead) {
            System.out.println("br=" + bytesRead + ",tr=" + toRead);
            long start = System.nanoTime();
            int rb = this.socketChannel.read(buffer);
            final long end = System.nanoTime();
            System.out.println("Read " + rb + " bytes in " + (end - start) + "ns");
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

/*
    public boolean readAtLeast(final ByteBuffer buffer, int size, int readCount) throws IOException {
        if (buffer.remaining() == 0) {
            buffer.clear();

        } else {
            buffer.compact();
        }

        int spins = 0;
        int bytes = 0;
        int targetPosition = buffer.position() + size;
        while (buffer.position() < targetPosition) {
            int rb = this.socketChannel.read(buffer);
            if (rb < 0) {
                throw new EOFException();

            } else if(bytes == 0 && rb == 0 && readCount > -1) {
                if(spins++ >= readCount) {
                    return false;
                }
            } else {
                spins = 0;
                bytes += rb;
            }
        }

        buffer.flip();

        return true;
    }
*/

    private boolean readFullyOrEOF(@NotNull ByteBuffer bb) throws IOException {
        while (bb.remaining() > 0) {
            if (this.socketChannel.read(bb) < 0) {
                break;
            }
        }

//        System.out.println("r - "+ChronicleTools.asString(bb));
        if (bb.remaining() > 0) {
            throw new EOFException();
        }

        bb.flip();
        return true;
    }

    public boolean readUpTo(Bytes bytes, int size, int readAttempts) throws IOException {
        return readUpTo(bytes.sliceAsByteBuffer(buffer), size, readAttempts);
    }

    public boolean readUpTo(ByteBuffer buffer, int size, int readAttempts) throws IOException {
        buffer.clear();
        buffer.limit(size);

        return readAttempts == -1 ? readFullyOrEOF(buffer) : read0(buffer, size, readAttempts);
    }

    public void writeSizeAndIndex(ByteBuffer buffer, int size, long index) throws IOException {
        buffer.clear();
        buffer.putInt(size);
        buffer.putLong(index);
        buffer.flip();

        writeAllOrEOF(buffer);
    }

    public void writeAction(ByteBuffer buffer, long action, long index) throws IOException {
        buffer.clear();
        buffer.putLong(action);
        buffer.putLong(index);
        buffer.flip();

        writeAllOrEOF(buffer);
    }
}
