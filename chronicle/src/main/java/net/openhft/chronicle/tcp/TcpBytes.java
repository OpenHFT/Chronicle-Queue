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

import net.openhft.lang.io.IOTools;
import net.openhft.lang.io.NativeBytes;
import net.openhft.lang.io.serialization.impl.VanillaBytesMarshallerFactory;
import net.openhft.lang.model.constraints.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;

public class TcpBytes extends NativeBytes {
    private ByteBuffer buffer;

    public TcpBytes(int initialSize) {
        super(new VanillaBytesMarshallerFactory(), NO_PAGE, NO_PAGE, null);

        this.buffer       = ChronicleTcp.createBufferOfSize(initialSize);
        this.startAddr    = ChronicleTcp.address(this.buffer);
        this.positionAddr = this.startAddr;
        this.capacityAddr = this.startAddr + initialSize;
        this.limitAddr    = this.startAddr + initialSize;
    }

    public TcpBytes resize(long size) {
        if(size > buffer.capacity()) {
            IOTools.clean(buffer);

            this.buffer       = ChronicleTcp.createBufferOfSize((int)size);
            this.startAddr    = ChronicleTcp.address(this.buffer);
            this.positionAddr = this.startAddr;
            this.capacityAddr = this.startAddr + size;
            this.limitAddr    = this.startAddr + size;
        }

        return this;
    }

    public void send(@NotNull TcpConnection cnx) throws IOException {
        buffer.clear();
        buffer.limit((int) limit());
        buffer.flip();

        cnx.writeAllOrEOF(buffer);
    }

    public void read(@NotNull TcpConnection cnx, int size) throws IOException {
        resize(size);

        buffer.clear();
        buffer.limit(size);
        cnx.readFullyOrEOF(buffer);
        buffer.flip();

        this.positionAddr = this.startAddr;
        this.limitAddr    = this.startAddr + buffer.remaining();
    }

    @Override
    public void close() {
        super.close();

        IOTools.clean(buffer);

        this.startAddr    = NO_PAGE;
        this.positionAddr = NO_PAGE;
        this.capacityAddr = NO_PAGE;
        this.limitAddr    = NO_PAGE;
    }
}
