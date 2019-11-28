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

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
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
    int TEST_BLOCK_SIZE = 64 * 1024; // smallest safe block size for Windows 8+

    static ChronicleQueue single(String path) {
        return SingleChronicleQueueBuilder.single(path).build();
    }

    static SingleChronicleQueueBuilder singleBuilder() {
        return SingleChronicleQueueBuilder.single();
    }

    static SingleChronicleQueueBuilder singleBuilder(String path) {
        return SingleChronicleQueueBuilder.binary(path);
    }

    static SingleChronicleQueueBuilder singleBuilder(File path) {
        return SingleChronicleQueueBuilder.binary(path);
    }

    static SingleChronicleQueueBuilder singleBuilder(Path path) {
        return SingleChronicleQueueBuilder.binary(path);
    }

    /**
     * <b>
     * Tailers are NOT thread-safe, sharing the Tailer between threads will lead to errors and unpredictable behaviour.
     * </b>
     *
     * @return a new ExcerptTailer to read sequentially.
     */
    @NotNull
    ExcerptTailer createTailer();

    /**
     * <b>
     * Tailers are NOT thread-safe, sharing the Tailer between threads will lead to errors and unpredictable behaviour.
     * </b>
     *
     * @param id unique id for a tailer which uses to track where it was up to
     * @return a new ExcerptTailer to read sequentially.
     */
    @NotNull
    default ExcerptTailer createTailer(String id) {
        throw new UnsupportedOperationException("not currently supported in this implementation.");
    }

    /**
     * <p>
     * An Appender can be used to writeBytes new excerpts sequentially to the upper.
     * </p>
     * <p>
     * Appenders are NOT thread-safe, sharing the Appender between threads will lead to errors and unpredictable behaviour.
     * This method returns {@link ThreadLocal} appender, so does not produce any garbage, hence it's safe to simply call
     * this method every time an appender is needed.
     * </p>
     *
     * @return A thread local Appender for writing new entries to the end.
     */
    @NotNull
    ExcerptAppender acquireAppender();

    /**
     * @deprecated to be remove in version 4.6 or later use {@link ChronicleQueue#acquireAppender()}
     */
    @NotNull
    @Deprecated
    default ExcerptAppender createAppender() {
        return acquireAppender();
    }

    /**
     * @return the lowest valid index available, or Long.MAX_VALUE if none are found
     */
    long firstIndex();

    /**
     * @return the type of wire used, for example WireTypes.TEXT or WireTypes.BINARY
     */
    @NotNull
    WireType wireType();

    /**
     * Remove all the entries in the queue.
     */
    void clear();

    /**
     * @return the base file where ChronicleQueue stores its data.
     */
    @NotNull
    File file();

    /**
     * Cache this value as getAbsolutePath is expensive
     *
     * @return the absolute path of the file where ChronicleQueue stores its data.
     */
    @NotNull
    default String fileAbsolutePath() {
        return file().getAbsolutePath();
    }

    /**
     * Dump a Queue in YAML format.
     *
     * @return the contents of the Queue as YAML.
     */
    @NotNull
    String dump();

    /**
     * Dump a range of entries to a Writer
     *
     * @param writer    to write to
     * @param fromIndex first index to include
     * @param toIndex   last index to include.
     */
    void dump(Writer writer, long fromIndex, long toIndex);

    default void dump(@NotNull OutputStream stream, long fromIndex, long toIndex) {
        dump(new OutputStreamWriter(stream, StandardCharsets.UTF_8), fromIndex, toIndex);
    }

    int sourceId();

    /**
     * NOTE the writer generated is not thread safe, you need to keep a ThreadLocal of these if needed.
     */
    @SuppressWarnings("unchecked")
    default <T> T methodWriter(@NotNull Class<T> tClass, Class... additional) {
        VanillaMethodWriterBuilder<T> builder = methodWriterBuilder(tClass);
        Stream.of(additional).forEach(builder::addInterface);
        return builder.build();
    }

    @NotNull
    default <T> VanillaMethodWriterBuilder<T> methodWriterBuilder(@NotNull Class<T> tClass) {
        return new VanillaMethodWriterBuilder<T>(tClass,
                () -> new BinaryMethodWriterInvocationHandler(false, this::acquireAppender));
    }

    RollCycle rollCycle();

    TimeProvider time();

    int deltaCheckpointInterval();

    /**
     * when using replication to another host, this is the last index that has been sent to the remote host.
     */
    long lastIndexReplicated();

    long lastAcknowledgedIndexReplicated();

    /**
     * @param lastIndex last index that has been sent to the remote host.
     */
    void lastIndexReplicated(long lastIndex);

    void lastAcknowledgedIndexReplicated(long lastAcknowledgedIndexReplicated);

    /**
     * call this method if you delete file from a chronicle-queue directory
     * <p>
     * The problem that we have, is that we cache the structure of your directory, this is because
     * hitting the file system adds latency. Call this method, if you delete .cq4 files, then it
     * will update our caches accordingly,
     */
    void refreshDirectlyListing();

    String dumpLastHeader();
}
