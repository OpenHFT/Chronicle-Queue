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
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.core.util.ObjectUtils;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;

import java.io.StreamCorruptedException;
import java.lang.reflect.Proxy;
import java.util.Map;

/**
 * The component that facilitates sequentially writing data to a {@link ChronicleQueue}.
 *
 * @author peter.lawrey
 */
public interface ExcerptAppender extends ExcerptCommon {

    DocumentContext writingDocument();

    /**
     * @param writer to write to excerpt.
     */
    void writeDocument(@NotNull WriteMarshallable writer);

    /**
     * @param marshallable to write to excerpt.
     */
    void writeBytes(@NotNull WriteBytesMarshallable marshallable);

    /**
     * @param bytes to write to excerpt.
     */
    void writeBytes(@NotNull Bytes<?> bytes);

    /**
     * @param text to write a message
     */
    default void writeText(CharSequence text) {
        writeBytes(Bytes.from(text));
    }

    /**
     * Write an entry at a given index. This can use used for rebuilding a queue, or replication.
     *
     * @param index to write the byte to or fail.
     * @param bytes to write.
     * @throws StreamCorruptedException the write failed is was unable to write the data at the given index.
     */
    default void writeBytes(long index, Bytes<?> bytes) throws StreamCorruptedException {
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
     * Proxy an interface so each message called is written to a file for replay.
     *
     * @param tClass     primary interface
     * @param additional any additional interfaces
     * @return a proxy which implements the primary interface (additional interfaces have to be cast)
     */
    default <T> T methodWriter(Class<T> tClass, Class... additional) {
        Class[] interfaces = ObjectUtils.addAll(tClass, additional);

        //noinspection unchecked
        return (T) Proxy.newProxyInstance(tClass.getClassLoader(), interfaces, new MethodWriterInvocationHandler(this));
    }

    /**
     * Write a Map as a marshallable
     */
    default void writeMap(Map<String, Object> map) {
        QueueInternal.writeMap(this, map);
    }
}
