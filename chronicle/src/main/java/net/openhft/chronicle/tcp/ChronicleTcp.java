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
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;

public class ChronicleTcp {
    public static final Logger LOG = LoggerFactory.getLogger(ChronicleTcp.class);

    public static final int HEADER_SIZE = 12;
    public static final int INITIAL_BUFFER_SIZE = 64 * 1024;
    public static final int IN_SYNC_LEN = -128;
    public static final int PADDED_LEN = -127;
    public static final int SYNC_IDX_LEN = -126;

    // *************************************************************************
    //
    // *************************************************************************

    public static final class Command {
        public static final long ACTION_SUBSCRIBE = 1;
        public static final long ACTION_QUERY     = 2;

        private ByteBuffer buffer;
        private long action;
        private long data;

        public Command() {
            this(0, 0);
        }

        private Command(long action, long data) {
            this.action = action;
            this.data = data;

            this.buffer = ByteBuffer
                .allocate(16)
                .order(ByteOrder.nativeOrder())
                .putLong(action)
                .putLong(data);
        }

        public long action() {
            return this.action;
        }

        public long data() {
            return this.data;
        }

        public boolean read(final SocketChannel channel) throws IOException {
            this.buffer.clear();

            readFullyOrEOF(channel, this.buffer);

            this.buffer.flip();

            this.action = buffer.getLong();
            this.data   = buffer.getLong();

            return true;
        }

        public boolean write(final SocketChannel channel) throws IOException {
            buffer.flip();
            writeAllOrEOF(channel, this.buffer);
            return true;
        }

        public boolean isSubscribe() {
            return this.action == ACTION_SUBSCRIBE;
        }

        public boolean isQuery() {
            return this.action == ACTION_QUERY;
        }

        public static Command make(long action, long data) {
            return new Command(action, data);
        }

        public static boolean makeAndSend(long action, long data, final SocketChannel channel) throws IOException {
            return new Command(action, data).write(channel);
        }
    }

    // *************************************************************************
    //
    // *************************************************************************

    public static ByteBuffer createBuffer(int minSize, ByteOrder byteOrder) {
        int newSize = (minSize + INITIAL_BUFFER_SIZE - 1) / INITIAL_BUFFER_SIZE * INITIAL_BUFFER_SIZE;
        return ByteBuffer.allocateDirect(newSize).order(byteOrder);
    }


    public static void writeAllOrEOF(@NotNull SocketChannel sc, @NotNull ByteBuffer bb) throws IOException {
        writeAll(sc, bb);

        if (bb.remaining() > 0) {
            throw new EOFException();
        }
    }

    public static void writeAll(@NotNull SocketChannel sc, @NotNull ByteBuffer bb) throws IOException {
        while (bb.remaining() > 0) {
            if (sc.write(bb) < 0) {
                break;
            }
        }
    }

    public static void readFullyOrEOF(@NotNull SocketChannel socket, @NotNull ByteBuffer bb) throws IOException {
        readAvailable(socket, bb);
        if (bb.remaining() > 0) {
            throw new EOFException();
        }
    }

    private static void readAvailable(@NotNull SocketChannel socket, @NotNull ByteBuffer bb) throws IOException {
        while (bb.remaining() > 0) {
            if (socket.read(bb) < 0) {
                break;
            }
        }
    }

    public static boolean isLocalhost(final InetAddress address) {
        if(address.isLoopbackAddress()) {
            return true;
        }

        try {
            return NetworkInterface.getByInetAddress(address) != null;
        } catch(Exception e)  {
        }

        return false;
    }
}
