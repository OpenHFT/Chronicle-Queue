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

import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.File;

/**
 * <em>Chronicle</em> (in a generic sense) is a Java project focused on building a persisted low
 * latency messaging framework for high performance and critical applications.
 *
 * <p> Using non-heap storage options <em>Chronicle</em> provides a processing environment where
 * applications does not suffer from <em>GarbageCollection</em>. <em>GarbageCollection</em> (GC) may
 * slow down your critical operations non-deterministically at any time.. In order to avoid
 * non-determinism and escape from GC delays off-heap memory solutions are addressed. The main idea
 * is to manage your memory manually so does not suffer from GC. Chronicle behaves like a management
 * interface over off-heap memory so you can build your own solutions over it.
 *
 * <p><em>Chronicle</em> uses RandomAccessFiles while managing memory and this choice brings lots of
 * possibility. Random access files permit non-sequential, or random, access to a file's contents.
 * To access a file randomly, you open the file, seek a particular location, and read from or
 * writeBytes to that file. RandomAccessFiles can be seen as "large" C-type byte arrays that you can
 * access any random index "directly" using pointers. File portions can be used as ByteBuffers if
 * the portion is mapped into memory.
 *
 * <p>{@link ChronicleQueue} (now in the specific sense) is the main interface for management and
 * can be seen as the "Collection class" of the <em>Chronicle</em> environment. You will reserve a
 * portion of memory and then put/fetch/update records using the {@link ChronicleQueue}
 * interface.</p>
 *
 * <p>{@link Excerpt} is the main data container in a {@link ChronicleQueue}, each Chronicle is
 * composed of Excerpts. Putting data to a queue means starting a new Excerpt, writing data into it
 * and finishing the Excerpt at the upper.</p>
 *
 * <p>While {@link Excerpt} is a generic purpose container allowing for remote access, it also has
 * more specialized counterparts for sequential operations. See {@link ExcerptTailer} and {@link
 * net.openhft.chronicle.queue.ExcerptAppender}</p>
 *
 * @author peter.lawrey
 */
public interface ChronicleQueue extends Closeable {
    /**
     * An Excerpt can be used access entries randomly and optionally change them.
     *
     * @return Excerpt
     */
    @NotNull
    Excerpt createExcerpt();

    /**
     * A Tailer can be used to read sequentially from the lower of a given position.
     *
     * @return ExcerptTailer
     */
    @NotNull
    ExcerptTailer createTailer();

    /**
     * An Appender can be used to writeBytes new excerpts sequentially to the upper.
     *
     * @return ExcerptAppender
     */
    @NotNull
    ExcerptAppender createAppender();

    /**
     * @return the lowest valid index available, or sequenceNumber=0 if none are found
     */
    long firstIndex();

    /**
     * @return the highest valid index immediately available. Or -1 if none available.
     *
     * The lowest 40bits of the index refer to the sequence number with the cycle, giving a maximum
     * of 1099511627776 excerpt per cycle. Each cycle has its own file. Each file holds its own
     * index. You can discard the old files ( if they are no longer used ). The other highest 24
     * bits  of the index are used for the cycle number (this equates to the filename), giving a
     * maximum  of 16777216 cycles ( aka files )
     */
    long lastIndex();

    /**
     * @return the type of wire used, for example TEXT_WIRE or BINARY WIRE
     */
    @NotNull
    WireType wireType();

    /**
     * Remove all the entries in the queue.
     */
    void clear();

    /**
     * @return the base path where ChronicleQueue stores its data.
     */
    @NotNull
    File path();

}
