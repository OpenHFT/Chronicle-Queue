/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.MarshallableOut;
import net.openhft.chronicle.wire.UnrecoverableTimeoutException;
import org.jetbrains.annotations.NotNull;

import java.io.StreamCorruptedException;

/**
 * The component that facilitates sequentially writing data to a {@link ChronicleQueue}.
 *
 * @author peter.lawrey
 */
public interface ExcerptAppender extends ExcerptCommon<ExcerptAppender>, MarshallableOut {

    /**
     * @param bytes to write to excerpt.
     */
    void writeBytes(@NotNull Bytes<?> bytes) throws UnrecoverableTimeoutException;

    /**
     * Write an entry at a given index. This can use used for rebuilding a queue, or replication.
     *
     * @param index to write the byte to or fail.
     * @param bytes to write.
     * @throws StreamCorruptedException the write failed is was unable to write the data at the
     *                                  given index.
     */
  /*  default void writeBytes(long index, BytesStore bytes) throws StreamCorruptedException {
        throw new UnsupportedOperationException();
    }
*/

    /**
     * Write an entry at a given index. This can use used for rebuilding a queue, or replication.
     *
     * @param index to write the byte to or fail.
     * @return DocumentContext to write to.
     * @throws StreamCorruptedException the write failed is was unable to write the data at the
     *                                  given index.
     */
    default DocumentContext writingDocument(long index) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return the index last written, this index includes the cycle and the sequence number
     * @throws IllegalStateException if no index is available
     */
    long lastIndexAppended();

    /**
     * @return the cycle this tailer is on, usually with chronicle-queue each cycle will have its
     * own unique data file to store the excerpt
     */
    int cycle();

    /**
     * Asynchronous call to load a block before it  needed to reduce latency.
     */
    default void pretouch() {
    }

    /**
     * Enable padding if near the end of a cache line, pad it so a following 4-byte int value will
     * not split a cache line.
     */
    void padToCacheAlign(Padding padToCacheAlign);

    Padding padToCacheAlign();

}
