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

import net.openhft.lang.model.constraints.NotNull;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;

public class ChronicleTcp2 {
    public static final int HEADER_SIZE = 12;
    public static final int INITIAL_BUFFER_SIZE = 64 * 1024;
    public static final int IN_SYNC_LEN = -128;
    public static final int PADDED_LEN = -127;
    public static final int SYNC_IDX_LEN = -126;

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

    public static String connectionName(String name, final InetSocketAddress bindAddress, final InetSocketAddress connectAddress) {
        StringBuilder sb = new StringBuilder(name);
        if (bindAddress != null && connectAddress != null) {
            sb.append("[");
            sb.append(bindAddress.toString());
            sb.append(" -> ");
            sb.append(connectAddress.toString());
            sb.append("]");
        } else if (bindAddress != null) {
            sb.append("[");
            sb.append(bindAddress.toString());
            sb.append("]");
        } else if (connectAddress != null) {
            sb.append("[");
            sb.append(connectAddress.toString());
            sb.append("]");
        }

        return sb.toString();
    }
}
