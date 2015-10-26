/*
 *
 *    Copyright (C) 2015  higherfrequencytrading.com
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU Lesser General Public License as published by
 *    the Free Software Foundation, either version 3 of the License.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Lesser General Public License for more details.
 *
 *    You should have received a copy of the GNU Lesser General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */
package net.openhft.chronicle.queue.impl.ringbuffer;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.NativeBytesStore;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

public class BytesRingBufferTest {

    @Ignore
    @Test
    public void testOffer() throws IOException, InterruptedException {
        NativeBytesStore allocate = NativeBytesStore.nativeStoreWithFixedCapacity(1024);
        NativeBytesStore msgBytes = NativeBytesStore.nativeStoreWithFixedCapacity(150);

        BytesRingBuffer ring = new BytesRingBuffer(allocate.bytesForWrite());
        Bytes buffer = msgBytes.bytesForWrite();

        buffer.clear();
        buffer.writeLong(1L);
        ring.offer(buffer);
    }
}
