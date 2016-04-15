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

import net.openhft.lang.io.DirectByteBufferBytes;
import net.openhft.lang.model.constraints.NotNull;

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

    public SocketChannel socketChannel() {
        return this.socketChannel;
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

    public String debugString() {
        if(this.socketChannel != null) {
            try {
                StringBuilder sb = new StringBuilder();
                sb.append("[");
                sb.append(this.socketChannel.getLocalAddress());
                sb.append(" -> ");
                sb.append(this.socketChannel.getRemoteAddress());
                sb.append("]");
            } catch(IOException e) {
            }
        }

         return "[] -> []";
    }

    public int write(final ByteBuffer buffer) throws IOException {
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

    public void writeAll(final DirectByteBufferBytes bb) throws IOException {
        writeAll(bb.buffer());
    }

    public void writeAll(final ByteBuffer bb) throws IOException {
        int bw = 0;
        while (bb.remaining() > 0) {
            bw = this.socketChannel.write(bb);
            if (bw < 0) {
                break;
            }
        }
    }

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

    public boolean read(final ByteBuffer buffer, int threshod, int size, int readCount) throws IOException {
        int rem = buffer.remaining();
        if (rem < threshod) {
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
                        buffer.flip();
                        return false;
                    }
                } else {
                    spins = 0;
                    bytes += rb;
                }
            }

            buffer.flip();
        }

        return true;
    }

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

    public boolean readAllOrNone(final ByteBuffer buffer, int readCount) throws IOException {
        int spins = 0;
        int bytes = 0;
        while (buffer.remaining() > 0) {
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

        return true;
    }

    public void readFullyOrEOF(@NotNull ByteBuffer bb) throws IOException {
        readAvailable(bb);

//        System.out.println("r - "+ChronicleTools.asString(bb));
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

    public boolean readUpTo(ByteBuffer buffer, int size, int readCount) throws IOException {
        buffer.clear();
        buffer.limit(size);

        if(readCount == -1) {
            readFullyOrEOF(buffer);

        } else {
            if(!readAllOrNone(buffer, readCount)) {
                buffer.clear();
                return false;
            }
        }

        buffer.flip();

        return true;
    }

    public void writeSizeAndIndex(ByteBuffer buffer, int action, long index) throws IOException {
        buffer.clear();
        buffer.putInt(action);
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
