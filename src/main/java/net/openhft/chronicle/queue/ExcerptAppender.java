/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

    default <T> T methodWriter(Class<T> tClass, Class... additional) {
        Class[] interfaces = ObjectUtils.addAll(tClass, additional);

        //noinspection unchecked
        return (T) Proxy.newProxyInstance(tClass.getClassLoader(), interfaces, new MethodWriterInvocationHandler(this));
    }

}
