/*
 * Copyright 2013 Peter Lawrey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.tcp;

import net.openhft.lang.model.constraints.NotNull;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;

/**
 * @author peter.lawrey
 */
public enum TcpUtil {
    ;
    public static final int HEADER_SIZE = 12;
    private static final int INITIAL_BUFFER_SIZE = 64 * 1024;

    public static ByteBuffer createBuffer(int minSize, ByteOrder byteOrder) {
        int newSize = (minSize + INITIAL_BUFFER_SIZE - 1) / INITIAL_BUFFER_SIZE * INITIAL_BUFFER_SIZE;
        return ByteBuffer.allocateDirect(newSize).order(byteOrder);
    }


    public static void writeAllOrEOF(@NotNull SocketChannel sc, @NotNull ByteBuffer bb) throws IOException {
        writeAll(sc, bb);

        if (bb.remaining() > 0) throw new EOFException();
    }

    public static void writeAll(@NotNull SocketChannel sc, @NotNull ByteBuffer bb) throws IOException {
        while (bb.remaining() > 0)
            if (sc.write(bb) < 0)
                break;
    }

    public static void readFullyOrEOF(@NotNull SocketChannel socket, @NotNull ByteBuffer bb) throws IOException {
        readAvailable(socket, bb);
        if (bb.remaining() > 0) throw new EOFException();
    }

    private static void readAvailable(@NotNull SocketChannel socket, @NotNull ByteBuffer bb) throws IOException {
        while (bb.remaining() > 0)
            if (socket.read(bb) < 0)
                break;
    }
}
