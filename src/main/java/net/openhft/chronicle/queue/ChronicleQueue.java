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
import java.io.IOException;

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

    long MINUS_1_40BIT = toSequenceNumber(-1);

    /**
     * @return A descriptive name for this queue which can be used for logging.
     */
    @NotNull
    String name();

    /**
     * An Excerpt can be used access entries randomly and optionally change them.
     *
     * @return Excerpt
     * @throws IOException if an IO problem occurs
     */
    @NotNull
    Excerpt createExcerpt() throws IOException;

    /**
     * A Tailer can be used to read sequentially from the lower of a given position.
     *
     * @return ExcerptTailer
     * @throws IOException if an IO problem occurs
     */
    @NotNull
    ExcerptTailer createTailer() throws IOException;

    /**
     * An Appender can be used to writeBytes new excerpts sequentially to the upper.
     *
     * @return ExcerptAppender
     * @throws IOException if an IO problem occurs
     */
    @NotNull
    ExcerptAppender createAppender() throws IOException;

    /**
     * @return The current estimated number of entries.
     */
    long size();

    /**
     * Remove all the entries in the queue.
     */
    void clear();


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
    WireType wireType();

    static long toSequenceNumber(long index) {
        final long l = index & 0xFFFFFFFFFFL;
        return (l == MINUS_1_40BIT) ? -1 : l;
    }

    static long toCycle(long index) {
        int result = (int) (index >> 40L);
        if (result > (1 << 24))
            throw new IllegalStateException("cycle value is too large, it must fit into 24bits, " +
                    "either use a larger rollType of increase the roll offset.");

        if (result < 0)
            throw new IllegalStateException("Invalid cycle=" + result + ", cycles can not be negative" +
                    ".");
        return result;
    }

    static long index(long cycle, long sequenceNumber) {
        return (cycle << 40L) + (sequenceNumber & 0xFFFFFFFFFFL);
    }
}
