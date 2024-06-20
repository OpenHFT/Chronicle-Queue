/*
 * Copyright 2014 Higher Frequency Trading
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

package net.openhft.chronicle.queue.reader;

import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;

import java.nio.file.Path;
import java.util.function.Consumer;

/**
 * The Reader interface provides methods for reading messages from a Chronicle Queue.
 * It allows for customization of the reading process through various configuration methods, and
 * creates a Reader with the {@link #create()} method
 */
public interface Reader {

    /**
     * Executes the Reader.
     */
    void execute();

    /**
     * Stops the Reader.
     */
    void stop();

    /**
     * Sets the message sink for this Reader. If not set, messages are output to stdout.
     *
     * @param messageSink A Consumer function that will handle the messages read by this Reader.
     * @return this
     */
    Reader withMessageSink(@NotNull Consumer<String> messageSink);

    /**
     * Sets the base path for this Reader.
     *
     * @param path The base path.
     * @return this
     */
    Reader withBasePath(@NotNull Path path);

    /**
     * Adds an inclusion regex for this Reader. These are anded together.
     *
     * @param regex The inclusion regex.
     * @return The Reader instance with the inclusion regex set.
     */
    Reader withInclusionRegex(@NotNull String regex);

    /**
     * Adds exclusion regex for this Reader. These are anded together.
     *
     * @param regex The exclusion regex.
     * @return this
     */
    Reader withExclusionRegex(@NotNull String regex);

    /**
     * Sets the custom plugin for this Reader. Allows more flexibility than {@link #withMessageSink(Consumer)}
     *
     * @param customPlugin The custom plugin.
     * @return this
     */
    Reader withCustomPlugin(@NotNull ChronicleReaderPlugin customPlugin);

    /**
     * Sets the start index for this Reader.
     *
     * @param index The start index.
     * @return this
     */
    Reader withStartIndex(final long index);

    /**
     * Sets the content-based limiter for this Reader.
     *
     * @param contentBasedLimiter The content-based limiter.
     * @return this
     */
    ChronicleReader withContentBasedLimiter(ContentBasedLimiter contentBasedLimiter);

    /**
     * Sets the argument for this Reader. Used in conjunction with {@link #withBinarySearch(String)}
     *
     * @param arg The argument.
     * @return this
     */
    Reader withArg(@NotNull String arg);

    /**
     * Sets the limiter argument for this Reader. Used with {@link #withContentBasedLimiter(ContentBasedLimiter)}
     *
     * @param limiterArg The limiter argument.
     * @return this
     */
    Reader withLimiterArg(@NotNull String limiterArg);

    /**
     * Sets the Reader to tail mode.
     *
     * @return this
     */
    Reader tail();

    /**
     * Sets the maximum number of history records for this Reader.
     *
     * @param maxHistoryRecords The maximum number of history records.
     * @return this
     */
    Reader historyRecords(final long maxHistoryRecords);

    /**
     * Sets the method reader interface for this Reader.
     *
     * @param methodReaderInterface The method reader interface class name. If empty, a dummy reader is created.
     * @return this
     */
    Reader asMethodReader(@NotNull String methodReaderInterface);

    /**
     * Sets the wire type for this Reader.
     *
     * @param wireType The wire type.
     * @return this
     */
    Reader withWireType(@NotNull WireType wireType);

    /**
     * Suppresses the display index for this Reader.
     *
     * @return this
     */
    Reader suppressDisplayIndex();

    /**
     * Sets the binary search for this Reader.
     *
     * @param binarySearch The binary search.
     * @return this
     */
    Reader withBinarySearch(@NotNull String binarySearch);

    /**
     * Sets whether to show message history for this Reader.
     *
     * @param showMessageHistory Whether to show message history.
     * @return this
     */
    Reader showMessageHistory(boolean showMessageHistory);

    /**
     * Retrieves the argument for this Reader.
     *
     * @return The argument.
     */
    String arg();

    /**
     * Retrieves the limiter argument for this Reader.
     *
     * @return The limiter argument.
     */
    String limiterArg();

    /**
     * Retrieves the method reader interface for this Reader.
     *
     * @return The method reader interface.
     */
    Class<?> methodReaderInterface();

    /**
     * Creates a new Reader instance.
     *
     * @return A new Reader instance.
     */
    static Reader create() {
        return new ChronicleReader();
    }
}
