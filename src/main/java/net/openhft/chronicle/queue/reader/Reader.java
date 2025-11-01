/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.reader;

import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;

import java.nio.file.Path;
import java.util.function.Consumer;

/**
 * The Reader interface provides methods for reading messages from a Chronicle Queue.
 * <p>
 * It allows for extensive customization of the reading process through various configuration methods,
 * including setting the base path, inclusion/exclusion filters, content-based limiters, and method reader interfaces.
 * A new Reader can be created using the {@link #create()} method.
 */
public interface Reader {

    /**
     * Executes the Reader to begin processing messages from the queue.
     */
    void execute();

    /**
     * Stops the Reader, halting further processing.
     */
    void stop();

    /**
     * Sets the message sink for this Reader. If not set, messages are output to stdout.
     *
     * @param messageSink A Consumer function that will handle the messages read by this Reader.
     * @return The current instance of {@link Reader}
     */
    Reader withMessageSink(@NotNull Consumer<String> messageSink);

    /**
     * Sets the base path for this Reader, which indicates the location of the Chronicle Queue.
     *
     * @param path The base path of the Chronicle Queue.
     * @return The current instance of {@link Reader}
     */
    Reader withBasePath(@NotNull Path path);

    /**
     * Adds an inclusion regex for filtering messages.
     * <p>Messages that match the inclusion regex will be processed.
     *
     * @param regex The inclusion regex.
     * @return The current instance of {@link Reader}
     */
    Reader withInclusionRegex(@NotNull String regex);

    /**
     * Adds an exclusion regex for filtering messages.
     * <p>Messages that match the exclusion regex will be filtered out.
     *
     * @param regex The exclusion regex.
     * @return The current instance of {@link Reader}
     */
    Reader withExclusionRegex(@NotNull String regex);

    /**
     * Sets a custom plugin for this Reader, allowing more flexibility than {@link #withMessageSink(Consumer)}.
     *
     * @param customPlugin The custom plugin to use.
     * @return The current instance of {@link Reader}
     */
    Reader withCustomPlugin(@NotNull ChronicleReaderPlugin customPlugin);

    /**
     * Sets the start index for reading messages from the queue.
     *
     * @param index The start index.
     * @return The current instance of {@link Reader}
     */
    Reader withStartIndex(final long index);

    /**
     * Sets the content-based limiter for this Reader to control the processing of messages based on their content.
     *
     * @param contentBasedLimiter The content-based limiter.
     * @return The current instance of {@link Reader}
     */
    ChronicleReader withContentBasedLimiter(ContentBasedLimiter contentBasedLimiter);

    /**
     * Sets an argument for this Reader, typically used in conjunction with {@link #withBinarySearch(String)}.
     *
     * @param arg The argument to pass.
     * @return The current instance of {@link Reader}
     */
    Reader withArg(@NotNull String arg);

    /**
     * Sets an argument for the content-based limiter in this Reader.
     *
     * @param limiterArg The limiter argument.
     * @return The current instance of {@link Reader}
     */
    Reader withLimiterArg(@NotNull String limiterArg);

    /**
     * Configures the Reader to operate in tail mode, continuously reading new messages as they arrive.
     *
     * @return The current instance of {@link Reader}
     */
    Reader tail();

    /**
     * Sets the maximum number of history records to read from the queue.
     *
     * @param maxHistoryRecords The maximum number of history records.
     * @return The current instance of {@link Reader}
     */
    Reader historyRecords(final long maxHistoryRecords);

    /**
     * Sets the method reader interface for this Reader.
     * <p>If the provided interface name is empty, a dummy method reader will be created.
     *
     * @param methodReaderInterface The fully qualified class name of the method reader interface.
     * @return The current instance of {@link Reader}
     */
    Reader asMethodReader(@NotNull String methodReaderInterface);

    /**
     * Sets the wire type for this Reader, determining how messages are serialized and deserialized.
     *
     * @param wireType The wire type.
     * @return The current instance of {@link Reader}
     */
    Reader withWireType(@NotNull WireType wireType);

    /**
     * Suppresses the display of the index in the output for this Reader.
     *
     * @return The current instance of {@link Reader}
     */
    Reader suppressDisplayIndex();

    /**
     * Sets the binary search functionality for this Reader, allowing it to search for specific entries.
     *
     * @param binarySearch The fully qualified class name of the binary search implementation.
     * @return The current instance of {@link Reader}
     */
    Reader withBinarySearch(@NotNull String binarySearch);

    /**
     * Enables or disables the display of message history for this Reader.
     *
     * @param showMessageHistory {@code true} to show message history, {@code false} otherwise.
     * @return The current instance of {@link Reader}
     */
    Reader showMessageHistory(boolean showMessageHistory);

    /**
     * Retrieves the argument set for this Reader.
     *
     * @return The argument as a string.
     */
    String arg();

    /**
     * Retrieves the argument set for the content-based limiter in this Reader.
     *
     * @return The limiter argument as a string.
     */
    String limiterArg();

    /**
     * Retrieves the method reader interface for this Reader.
     *
     * @return The method reader interface class.
     */
    Class<?> methodReaderInterface();

    /**
     * Creates a new instance of {@link Reader}.
     *
     * @return A new Reader instance.
     */
    static Reader create() {
        return new ChronicleReader();
    }
}
