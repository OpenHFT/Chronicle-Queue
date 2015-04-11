/*
 * Copyright 2015 Higher Frequency Trading
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

package net.openhft.chronicle.tools;

import net.openhft.lang.io.DirectByteBufferBytes;

import java.nio.ByteBuffer;

public class ResizableDirectByteBufferBytes extends DirectByteBufferBytes {
    public ResizableDirectByteBufferBytes(int capacity) {
        super(capacity);
    }

    public ResizableDirectByteBufferBytes(ByteBuffer buffer) {
        super(buffer);
    }

    public ResizableDirectByteBufferBytes(ByteBuffer buffer, int start, int capacity) {
        super(buffer, start, capacity);
    }

    public ResizableDirectByteBufferBytes resizeIfNeeded(int newCapacity) {
        if (capacity() < newCapacity) {
            resize(newCapacity, false, false);
        }

        return this;
    }

    public ResizableDirectByteBufferBytes resetToSize(int size) {
        resizeIfNeeded(size);

        clear();
        buffer().clear();
        limit(size);
        buffer().limit(size);

        return this;
    }
}
