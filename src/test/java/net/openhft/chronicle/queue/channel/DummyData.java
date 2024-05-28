/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue.channel;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.wire.BytesInBinaryMarshallable;
import net.openhft.chronicle.wire.converter.NanoTime;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;

public class DummyData extends BytesInBinaryMarshallable {
    @NanoTime
    long timeNS;
    byte[] data;

    public long timeNS() {
        return timeNS;
    }

    public DummyData timeNS(long timeNS) {
        this.timeNS = timeNS;
        return this;
    }

    public byte[] data() {
        return data;
    }

    public DummyData data(byte[] data) {
        this.data = data;
        return this;
    }

    @Override
    public void readMarshallable(BytesIn<?> bytes) throws IORuntimeException, BufferUnderflowException, IllegalStateException {
        timeNS = bytes.readLong();
        int len = bytes.readInt();
        if (len == -1) {
            data = null;
        } else {
            if (data == null || data.length != len)
                data = new byte[len];
            bytes.read(data);
        }
    }

    @Override
    public void writeMarshallable(BytesOut<?> bytes) throws IllegalStateException, BufferOverflowException, BufferUnderflowException, ArithmeticException {
        bytes.writeLong(timeNS);
        if (data == null) {
            bytes.writeInt(-1);
        } else {
            bytes.writeInt(data.length);
            bytes.write(data);
        }
    }
}
