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
import net.openhft.lang.model.constraints.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;

public class TcpFrame {
    private static final int HEADER_TYPE_SIZE = 4;
    private static final int HEADER_DATA_SIZE = 8;
    private static final int HEADER_SIZE      = HEADER_TYPE_SIZE + HEADER_DATA_SIZE;

    private ByteBuffer header;
    private ByteBuffer data;

    public TcpFrame() {
        this.header = ChronicleTcp.createBufferOfSize(HEADER_TYPE_SIZE);
        this.data   = null;
    }

    public TcpFrame withHeader(int headerType, long headerData) {
        this.header.clear();
        this.header.putInt(headerType);
        this.header.putLong(headerData);
        this.header.flip();

        return this;
    }

    public TcpFrame withData(Bytes bytes) {
        ensureDataCapacity((int)bytes.capacity());

        bytes.read(this.data);
        this.data.flip();

        return this;
    }

    public TcpFrame send(@NotNull TcpConnection connection) throws IOException {
        connection.writeAllOrEOF(this.header);
        if(this.data != null && this.data.remaining() > 0) {
            connection.writeAllOrEOF(this.data);
        }

        return this;
    }

    public TcpFrame readHeader(@NotNull TcpConnection connection) throws IOException {
        connection.readUpTo(this.header, HEADER_SIZE, -1);
        return this;
    }

    public TcpFrame readData(@NotNull TcpConnection connection, int size) throws IOException {
        ensureDataCapacity(size);

        connection.readUpTo(this.data, size, -1);
        return this;
    }

    public int headerType() {
        return this.header.getInt(0);
    }

    public long headerData() {
        return this.header.getLong(HEADER_TYPE_SIZE);
    }

    public long dataStartAddress() {
        return this.data != null ? ChronicleTcp.address(this.data) : 0;
    }

    public long dataCapacityAddress() {
        return this.data != null ? ChronicleTcp.address(this.data) + this.data.capacity() : 0;
    }

    // *************************************************************************
    //
    // *************************************************************************

    private void ensureDataCapacity(int size) {
        if(this.data == null || this.data.capacity() < size) {
            ChronicleTcp.clean(this.data);

            this.data = ChronicleTcp.createBufferOfSize(size);
            this.data.clear();
            this.data.limit(size);
        }
    }
}
