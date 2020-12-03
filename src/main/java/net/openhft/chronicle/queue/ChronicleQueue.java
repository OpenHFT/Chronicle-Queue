/*
 * Copyright 2016-2020 chronicle.software
 *
 * https://chronicle.software
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
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.util.QueueUtil;
import net.openhft.chronicle.wire.BinaryMethodWriterInvocationHandler;
import net.openhft.chronicle.wire.VanillaMethodWriterBuilder;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * <em>Chronicle</em> (in a generic sense) is a Java project focused on building a persisted low
 * latency messaging framework for high performance and critical applications.
 * <p> Using non-heap storage options <em>Chronicle</em> provides a processing environment where
 * applications does not suffer from <em>GarbageCollection</em>. <em>GarbageCollection</em> (GC) may
 * slow down your critical operations non-deterministically at any time.. In order to avoid
 * non-determinism and escape from GC delays off-heap memory solutions are addressed. The main idea
 * is to manage your memory manually so does not suffer from GC. Chronicle behaves like a management
 * interface over off-heap memory so you can build your own solutions over it.
 * <p><em>Chronicle</em> uses RandomAccessFiles while managing memory and this choice brings lots of
 * possibility. Random access files permit non-sequential, or random, access to a file's contents.
 * To access a file randomly, you open the file, seek a particular location, and read from or
 * writeBytes to that file. RandomAccessFiles can be seen as "large" C-type byte arrays that you can
 * access any random index "directly" using pointers. File portions can be used as ByteBuffers if
 * the portion is mapped into memory.
 * <p>{@link ChronicleQueue} (now in the specific sense) is the main interface for management and
 * can be seen as the "Collection class" of the <em>Chronicle</em> environment. You will reserve a
 * portion of memory and then put/fetch/update records using the {@link ChronicleQueue}
 * interface.</p>
 * <p>{@link ExcerptCommon} is the main data container in a {@link ChronicleQueue}, each Chronicle
 * is composed of Excerpts. Putting data to a queue means starting a new Excerpt, writing data into
 * it and finishing the Excerpt at the upper.</p>
 * <p>While {@link ExcerptCommon} is a generic purpose container allowing for remote access, it also
 * has more specialized counterparts for sequential operations. See {@link ExcerptTailer} and {@link
 * ExcerptAppender}</p>
 *
 * @author peter.lawrey
 */
public interface ChronicleQueue extends Closeable {

    @Deprecated /* For removal, use QueueUtil.testBlockSize instead */
    int TEST_BLOCK_SIZE = QueueUtil.testBlockSize();

    /**
     * Creates and returns a new {@link ChronicleQueue} that will be backed by
     * files located in the directory named by the provided {@code pathName}.
     *
     * @param pathName of the directory to use for storing the queue
     * @return a new {@link ChronicleQueue} that will be stored
     * in the directory given by the provided {@code pathName}
     * @throws NullPointerException if the provided {@code pathName} is {@code null}.
     */
    static ChronicleQueue single(@NotNull String pathName) {
        return SingleChronicleQueueBuilder.single(pathName).build();
    }

    /**
     * Creates and returns a new {@link SingleChronicleQueueBuilder}.
     * <p>
     * The builder can be used to build a ChronicleQueue.
     *
     * @return a new {@link SingleChronicleQueueBuilder}
     */
    static SingleChronicleQueueBuilder singleBuilder() {
        return SingleChronicleQueueBuilder.single();
    }

    /**
     * Creates and returns a new {@link SingleChronicleQueueBuilder} that will
     * be pre-configured to use files located in the directory named by the
     * provided {@code pathName}.
     *
     * @param pathName of the directory to pre-configure for storing the queue
     * @return a new {@link SingleChronicleQueueBuilder} that will
     * be pre-configured to use files located in the directory named by the
     * provided {@code pathName}
     * @throws NullPointerException if the provided {@code pathName} is {@code null}.
     */
    static SingleChronicleQueueBuilder singleBuilder(@NotNull String pathName) {
        return SingleChronicleQueueBuilder.binary(pathName);
    }

    /**
     * Creates and returns a new {@link SingleChronicleQueueBuilder} that will
     * be pre-configured to use files located in the directory of the
     * provided {@code path}.
     *
     * @param path of the directory to pre-configure for storing the queue
     * @return a new {@link SingleChronicleQueueBuilder} that will
     * be pre-configured to use files located in the directory named by the
     * provided {@code pathName}
     * @throws NullPointerException if the provided {@code path} is {@code null}.
     */
    static SingleChronicleQueueBuilder singleBuilder(@NotNull File path) {
        return SingleChronicleQueueBuilder.binary(path);
    }

    /**
     * Creates and returns a new {@link SingleChronicleQueueBuilder} that will
     * be pre-configured to use files located in the directory of the
     * provided {@code path}.
     *
     * @param path of the directory to pre-configure for storing the queue
     * @return a new {@link SingleChronicleQueueBuilder} that will
     * be pre-configured to use files located in the directory named by the
     * provided {@code pathName}
     * @throws NullPointerException if the provided {@code path} is {@code null}.
     */
    static SingleChronicleQueueBuilder singleBuilder(@NotNull Path path) {
        return SingleChronicleQueueBuilder.binary(path);
    }

    /**
     * Creates and returns a new ExcerptTailer for this ChronicleQueue.
     * <b>
     * Tailers are NOT thread-safe. Sharing a Tailer across threads will lead to errors and unpredictable behaviour.
     * </b>
     * <p>
     * The tailor is created at the start, so unless you are using named tailors,
     * this method is the same as calling `net.openhft.chronicle.queue.ChronicleQueue#createTailer(java.lang.String).toStart()`
     *
     * @return a new ExcerptTailer to read sequentially.
     * @see #createTailer(String)
     */
    @NotNull
    ExcerptTailer createTailer();

    /**
     * Creates and returns a new ExcerptTailer for this ChronicleQueue with the given unique {@code id}.
     * <p>
     * The id is used to persistently store the latest index for the trailer. Any new Trailer with
     * a previously used id will continue where the old one left off.
     * <b>
     * Tailers are NOT thread-safe. Sharing a Tailer across threads will lead to errors and unpredictable behaviour.
     * </b>
     * <p>
     * If the provided {@code id} is {@code null}, the Trailer will be unnamed and this is
     * equivalent to invoking {@link #createTailer()}.
     *
     * @param id unique id for a tailer which uses to track where it was up to
     * @return a new ExcerptTailer for this ChronicleQueue with the given unique {@code id}
     * @see #createTailer()
     */
    @NotNull
    default ExcerptTailer createTailer(String id) {
        throw new UnsupportedOperationException("not currently supported in this implementation.");
    }

    /**
     * Returns a ExcerptAppender for this ChronicleQueue that is local to the current Thread.
     * <p>
     * An Appender can be used to store new excerpts sequentially to the queue.
     * <p>
     * <b>
     * Appenders are NOT thread-safe. Sharing an Appender across threads will lead to errors and unpredictable behaviour.
     * </b>
     * <p>
     * This method returns a {@link ThreadLocal} appender, so does not produce any garbage, hence it's safe to simply call
     * this method every time an appender is needed.
     *
     * @return Returns a ExcerptAppender for this ChronicleQueue that is local to the current Thread
     */
    @NotNull
    ExcerptAppender acquireAppender();

    /**
     * @deprecated to be remove in version 4.6 or later use {@link ChronicleQueue#acquireAppender()}
     */
    @NotNull
    @Deprecated(/* remove in x.21*/)
    default ExcerptAppender createAppender() {
        return acquireAppender();
    }

    /**
     * Returns the lowest valid index available for this ChronicleQueue, or {@link Long#MAX_VALUE}
     * if no such index exists.
     *
     * @return the lowest valid index available for this ChronicleQueue, or {@link Long#MAX_VALUE}
     * if no such index exists
     */
    long firstIndex();

    /**
     * Returns the {@link WireType} used for this ChronicleQueue.
     * <p>
     * For example, the WireType could be WireTypes.TEXT or WireTypes.BINARY.
     *
     * @return Returns the wire type used for this ChronicleQueue
     * @see WireType
     */
    @NotNull
    WireType wireType();

    /**
     * Removes all the excerpts in the current ChronicleQueue.
     */
    void clear();

    /**
     * Returns the base directory where ChronicleQueue stores its data.
     *
     * @return the base directory where ChronicleQueue stores its data
     */
    @NotNull
    File file();

    /**
     * Returns the absolute path of the base directory where ChronicleQueue stores its data.
     * <p>
     * This value might be cached, as getAbsolutePath is expensive
     *
     * @return the absolute path of the base directory where ChronicleQueue stores its data
     */
    @NotNull
    default String fileAbsolutePath() {
        return file().getAbsolutePath();
    }

    /**
     * Creates and returns a new String representation of this ChronicleQueue in YAML-format.
     *
     * @return a new String representation of this ChronicleQueue in YAML-format
     */
    @NotNull
    String dump();

    /**
     * Dumps a representation of this ChronicleQueue to the provided {@code writer} in YAML-format.
     * Dumping will be made from the provided (@code fromIndex) (inclusive) to the provided
     * {@code toIndex} (inclusive).
     *
     * @param writer    to write to
     * @param fromIndex first index (inclusive)
     * @param toIndex   last index  (inclusive)
     * @throws NullPointerException if the provided {@code writer} is {@code null}
     */
    void dump(Writer writer, long fromIndex, long toIndex);

    /**
     * Dumps a representation of this ChronicleQueue to the provided {@code stream} in YAML-format.
     * Dumping will be made from the provided (@code fromIndex) (inclusive) to the provided
     * {@code toIndex} (inclusive).
     *
     * @param stream    to write to
     * @param fromIndex first index (inclusive)
     * @param toIndex   last index  (inclusive)
     * @throws NullPointerException if the provided {@code writer} is {@code null}
     */
    default void dump(@NotNull OutputStream stream, long fromIndex, long toIndex) {
        dump(new OutputStreamWriter(stream, StandardCharsets.UTF_8), fromIndex, toIndex);
    }

    /**
     * Returns the source id.
     * <p>
     * The source id is non-negative.
     *
     * @return the source id
     */
    int sourceId();

    /**
     * Creates and returns a new writer proxy for the given interface {@code tclass} and the given {@code additional }
     * interfaces.
     * <p>
     * When methods are invoked on the returned T object, messages will be put in the queue.
     * <b>
     * Writers are NOT thread-safe. Sharing a Writer across threads will lead to errors and unpredictable behaviour.
     * </b>
     *
     * @param tClass     of the main interface to be implemented
     * @param additional interfaces to be implemented
     * @param <T>        type parameter of the main interface
     * @return a new proxy for the given interface {@code tclass} and the given {@code additional }
     * interfaces
     * @throws NullPointerException if any of the provided parameters are {@code null}.
     */
    default <T> T methodWriter(@NotNull Class<T> tClass, Class... additional) {
        VanillaMethodWriterBuilder<T> builder = methodWriterBuilder(tClass);
        Stream.of(additional).forEach(builder::addInterface);
        return builder.build();
    }

    /**
     * Creates and returns a new writer proxy for the given interface {@code tclass}.
     * <p>
     * When methods are invoked on the returned T object, messages will be put in the queue.
     * <p>
     * <b>
     * Writers are NOT thread-safe. Sharing a Writer across threads will lead to errors and unpredictable behaviour.
     * </b>
     *
     * @param tClass of the main interface to be implemented
     * @param <T>    type parameter of the main interface
     * @return a new proxy for the given interface {@code tclass}
     * @throws NullPointerException if the provided parameter is {@code null}.
     */
    @NotNull
    default <T> VanillaMethodWriterBuilder<T> methodWriterBuilder(@NotNull Class<T> tClass) {
        VanillaMethodWriterBuilder<T> builder = new VanillaMethodWriterBuilder<>(tClass,
                wireType(),
                () -> new BinaryMethodWriterInvocationHandler(false, this::acquireAppender));
        builder.marshallableOutSupplier(this::acquireAppender);
        return builder;
    }

    /**
     * Returns the {@link RollCycle} for this ChronicleQueue.
     *
     * @return the {@link RollCycle} for this ChronicleQueue
     * @see RollCycle
     */
    RollCycle rollCycle();

    /**
     * Returns the {@link TimeProvider} for this ChronicleQueue.
     *
     * @return the {@link TimeProvider} for this ChronicleQueue
     * @see TimeProvider
     */
    TimeProvider time();

    /**
     * Returns the Delta Checkpoint Interval for this ChronicleQueue.
     * <p>
     * The value returned is always a power of two.
     *
     * @return the Delta Checkpoint Interval for this ChronicleQueue
     */
    int deltaCheckpointInterval();

    /**
     * Returns the last index that was replicated to a remote host. If no
     * such index exists, returns -1.
     * <p>
     * This method is only applicable for replicating queues.
     *
     * @return the last index that was replicated to a remote host
     */
    long lastIndexReplicated();

    /**
     * Returns the last index that was replicated and acknowledged by all remote hosts. If no
     * such index exists, returns -1.
     * <p>
     * This method is only applicable for replicating queues.
     *
     * @return the last index that was replicated and acknowledged by all remote hosts
     */
    long lastAcknowledgedIndexReplicated();

    /**
     * Sets the last index that has been sent to a remote host.
     *
     * @param lastIndex last index that has been sent to the remote host.
     * @see #lastIndexReplicated()
     */
    void lastIndexReplicated(long lastIndex);

    /**
     * Sets the last index that has been sent to a remote host.
     *
     * @param lastAcknowledgedIndexReplicated last acknowledged index that has been sent to the remote host(s).
     * @see #lastAcknowledgedIndexReplicated()
     */
    void lastAcknowledgedIndexReplicated(long lastAcknowledgedIndexReplicated);

    /**
     * Refreshed this ChronicleQueue's view of the directory used for storing files.
     * <p>
     * Invoke this method if you delete file from a chronicle-queue directory
     * <p>
     * The problem solved by this is that we cache the structure of the queue directory in order to reduce
     * file system adds latency. Calling this method, after deleting .cq4 files, will update the internal
     * caches accordingly,
     */
    void refreshDirectoryListing();

    /**
     * Creates and returns a new String representation of this ChronicleQueue's last header in YAML-format.
     *
     * @return a new String representation of this ChronicleQueue's last header in YAML-format
     */
    @NotNull
    String dumpLastHeader();
}
