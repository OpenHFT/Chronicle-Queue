/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.reader;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.TailerDirection;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.queue.impl.single.BinarySearch;
import net.openhft.chronicle.queue.impl.single.NotComparableException;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.internal.reader.InternalDummyMethodReaderQueueEntryHandler;
import net.openhft.chronicle.queue.internal.reader.MessageCountingMessageConsumer;
import net.openhft.chronicle.queue.internal.reader.PatternFilterMessageConsumer;
import net.openhft.chronicle.queue.internal.reader.queueentryreaders.CustomPluginQueueEntryReader;
import net.openhft.chronicle.queue.internal.reader.queueentryreaders.MethodReaderQueueEntryReader;
import net.openhft.chronicle.queue.internal.reader.queueentryreaders.VanillaQueueEntryReader;
import net.openhft.chronicle.queue.reader.comparator.BinarySearchComparator;
import net.openhft.chronicle.queue.util.ToolsUtil;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static net.openhft.chronicle.queue.TailerDirection.BACKWARD;
import static net.openhft.chronicle.queue.TailerDirection.FORWARD;

/**
 * Implementation of the {@link Reader} interface, providing functionality for reading messages from a
 * Chronicle Queue with various configurations such as filtering, tailing, and binary search support.
 * <p>
 * The {@link ChronicleReader} class is designed to handle different types of queue reading patterns,
 * including tailing (continuous reading of new entries), and allows users to specify inclusion/exclusion
 * filters, start indices, and message processing through customizable plugins.
 */
public class ChronicleReader implements Reader {
    private static final long UNSET_VALUE = Long.MIN_VALUE;

    private final List<Pattern> inclusionRegex = new ArrayList<>();
    private final List<Pattern> exclusionRegex = new ArrayList<>();
    private final Pauser pauser = Pauser.millis(1, 100);
    private Path basePath;
    private long startIndex = UNSET_VALUE;
    private boolean tailInputSource = false;
    private long maxHistoryRecords = UNSET_VALUE;
    private boolean readOnly = true;
    private ChronicleReaderPlugin customPlugin;
    private Consumer<String> messageSink;
    private Function<ExcerptTailer, DocumentContext> pollMethod = ExcerptTailer::readingDocument;
    private WireType wireType = WireType.TEXT;
    private Supplier<QueueEntryHandler> entryHandlerFactory = () -> QueueEntryHandler.messageToText(wireType);
    private boolean displayIndex = true;
    private Class<?> methodReaderInterface;
    private BinarySearchComparator binarySearch;
    private String arg;
    private boolean showMessageHistory;
    private volatile boolean running = true;
    private TailerDirection tailerDirection = TailerDirection.FORWARD;
    private long matchLimit = 0;
    private ContentBasedLimiter contentBasedLimiter;
    private String limiterArg;
    private String tailerId = null;

    static {
        ToolsUtil.warnIfResourceTracing();
    }

    private static boolean isSet(final long configValue) {
        return configValue != UNSET_VALUE;
    }

    /**
     * Executes the reader logic by creating the necessary queue, tailers, and entry readers,
     * and processing messages until the stop condition is met.
     */
    public void execute() {
        configureContentBasedLimiter();
        validateArgs();
        long lastObservedTailIndex;
        long highestReachedIndex = 0L;
        boolean isFirstIteration = true;
        boolean retryLastOperation;
        boolean queueHasBeenModified;
        do {
            try (final ChronicleQueue queue = createQueue();
                 final ExcerptTailer tailer = queue.createTailer(tailerId);
                 final ExcerptTailer toEndTailer = queue.createTailer()) {
                MessageHistory.emptyHistory();

                MessageCountingMessageConsumer messageConsumer = new MessageCountingMessageConsumer(matchLimit, createMessageConsumers());
                QueueEntryReader queueEntryReader = createQueueEntryReader(tailer, messageConsumer);

                do {
                    if (highestReachedIndex != 0L) {
                        tailer.moveToIndex(highestReachedIndex);
                    }
                    try {
                        moveToSpecifiedPosition(queue, tailer, isFirstIteration);
                        lastObservedTailIndex = tailer.index();
                        readWhileNotInterrupted(tailer, messageConsumer, queueEntryReader);
                    } finally {
                        highestReachedIndex = tailer.index();
                        isFirstIteration = false;
                    }
                    queueHasBeenModified = queueHasBeenModifiedSinceLastCheck(lastObservedTailIndex, toEndTailer);
                    retryLastOperation = false;
                    if (!running || messageConsumer.matchLimitReached())
                        return;
                } while (tailerDirection != BACKWARD && (tailInputSource || queueHasBeenModified));
            } catch (final RuntimeException e) {
                retryLastOperation = handleRuntimeException(e);
            } finally {
                MessageHistory.clear();
            }
        } while (retryLastOperation);

    }

    /**
     * Handles runtime exceptions, particularly {@link DateTimeParseException} caused by race conditions
     * between different roll cycles. It retries the operation if this specific exception is encountered.
     *
     * @param e The caught runtime exception
     * @return {@code true} if the operation should be retried, {@code false} otherwise
     */
    private static boolean handleRuntimeException(RuntimeException e) {
        if (e.getCause() instanceof DateTimeParseException) {
            return true;
        } else {
            throw e;
        }
    }

    /**
     * Reads from the queue while the thread is not interrupted, pausing or halting as needed based on
     * the tail input source and content-based limits.
     *
     * @param tailer          The tailer used for reading messages
     * @param messageConsumer The consumer for processed messages
     * @param queueEntryReader The entry reader for the queue
     */
    private void readWhileNotInterrupted(ExcerptTailer tailer, MessageCountingMessageConsumer messageConsumer, QueueEntryReader queueEntryReader) {
        while (!Thread.currentThread().isInterrupted()) {
            if (shouldHaltReadingDueToContentBasedLimit(tailer)) {
                running = false;
                break;
            }

            if (!queueEntryReader.read()) {
                if (tailInputSource) {
                    pauser.pause();
                }
                break;
            } else {
                if (messageConsumer.matchLimitReached()) {
                    break;
                }
            }
            pauser.reset();
        }
    }

    /**
     * Validates the arguments for the {@link ChronicleReader}.
     * <p>Throws an {@link IllegalArgumentException} if a named tailer is used with a read-only queue.
     */
    private void validateArgs() {
        if (tailerId != null && readOnly)
            throw new IllegalArgumentException("Named tailers only work on writable queues");
    }

    /**
     * Configures the content-based limiter if specified.
     * <p>This method ensures that the content-based limiter is properly initialized before queue processing.
     */
    private void configureContentBasedLimiter() {
        if (contentBasedLimiter != null) {
            contentBasedLimiter.configure(this);
        }
    }

    /**
     * Check if the content-based limit has been reached
     *
     * @param tailer The Tailer we're using to read the queue
     * @return true if we should halt reading, false otherwise
     */
    private boolean shouldHaltReadingDueToContentBasedLimit(ExcerptTailer tailer) {
        if (contentBasedLimiter == null) {
            return false;
        }
        long originalIndex = tailer.index();
        try (final DocumentContext documentContext = tailer.readingDocument()) {
            if (documentContext.isPresent()) {
                return contentBasedLimiter.shouldHaltReading(documentContext);
            }
            return false;
        } finally {
            tailer.moveToIndex(originalIndex);
        }
    }

    /**
     * Creates a {@link QueueEntryReader} for processing entries in the queue.
     * <p>This method chooses between a vanilla reader, a plugin-based reader, or a method reader based on the configuration.
     *
     * @param tailer           The {@link ExcerptTailer} for reading queue entries
     * @param messageConsumer  The {@link MessageConsumer} for processing queue entries
     * @return A configured {@link QueueEntryReader} instance
     */
    private QueueEntryReader createQueueEntryReader(ExcerptTailer tailer, MessageConsumer messageConsumer) {
        if (methodReaderInterface == null) {
            if (customPlugin == null) {
                return new VanillaQueueEntryReader(tailer, pollMethod, entryHandlerFactory.get(), messageConsumer);
            } else {
                return new CustomPluginQueueEntryReader(tailer, pollMethod, customPlugin, messageConsumer);
            }
        } else {
            return new MethodReaderQueueEntryReader(tailer, messageConsumer, wireType, methodReaderInterface, showMessageHistory);
        }
    }

    /**
     * Creates a chain of message consumers according to the configured inclusion and exclusion patterns.
     *
     * @return The head of the chain of message consumers
     */
    private MessageConsumer createMessageConsumers() {
        MessageConsumer tail = this::writeToSink;
        if (!exclusionRegex.isEmpty()) {
            tail = new PatternFilterMessageConsumer(exclusionRegex, false, tail);
        }
        if (!inclusionRegex.isEmpty()) {
            tail = new PatternFilterMessageConsumer(inclusionRegex, true, tail);
        }
        return tail;
    }

    /**
     * Writes the index and text of a queue entry to the message sink.
     *
     * @param index The index of the entry being processed
     * @param text  The content of the entry being processed
     * @return {@code true} after writing to the sink
     */
    private boolean writeToSink(long index, String text) {
        if (displayIndex)
            messageSink.accept("0x" + Long.toHexString(index) + ": ");
        if (!text.isEmpty())
            messageSink.accept(text);
        return true;
    }

    /**
     * Sets whether the {@link ChronicleReader} operates in read-only mode.
     *
     * @param readOnly {@code true} to enable read-only mode, {@code false} otherwise
     * @return The current instance of {@link ChronicleReader}
     */
    public ChronicleReader withReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
        return this;
    }

    /**
     * Sets the maximum number of matching records to read.
     *
     * @param matchLimit The maximum number of records to match
     * @return The current instance of {@link ChronicleReader}
     */
    public ChronicleReader withMatchLimit(long matchLimit) {
        this.matchLimit = matchLimit;
        return this;
    }

    /**
     * Sets the consumer for handling messages processed by the {@link ChronicleReader}.
     *
     * @param messageSink The consumer for processing message strings
     * @return The current instance of {@link ChronicleReader}
     */
    public ChronicleReader withMessageSink(final @NotNull Consumer<String> messageSink) {
        this.messageSink = messageSink;
        return this;
    }

    /**
     * Gets the current message sink for handling processed messages.
     *
     * @return The current message sink
     */
    public Consumer<String> messageSink() {
        return messageSink;
    }

    /**
     * Sets the base path for the {@link ChronicleQueue} that the reader will operate on.
     *
     * @param path The base directory path for the Chronicle Queue
     * @return The current instance of {@link ChronicleReader}
     */
    public ChronicleReader withBasePath(final @NotNull Path path) {
        this.basePath = path;
        return this;
    }

    /**
     * Adds an inclusion regex for filtering messages.
     *
     * @param regex The regex pattern for inclusion
     * @return The current instance of {@link ChronicleReader}
     */
    public ChronicleReader withInclusionRegex(final @NotNull String regex) {
        this.inclusionRegex.add(Pattern.compile(regex));
        return this;
    }

    /**
     * Adds an exclusion regex for filtering messages.
     *
     * @param regex The regex pattern for exclusion
     * @return The current instance of {@link ChronicleReader}
     */
    public ChronicleReader withExclusionRegex(final @NotNull String regex) {
        this.exclusionRegex.add(Pattern.compile(regex));
        return this;
    }

    /**
     * Sets a custom plugin to handle queue entries.
     *
     * @param customPlugin The {@link ChronicleReaderPlugin} to use
     * @return The current instance of {@link ChronicleReader}
     */
    public ChronicleReader withCustomPlugin(final @NotNull ChronicleReaderPlugin customPlugin) {
        this.customPlugin = customPlugin;
        return this;
    }

    /**
     * Sets the start index for reading the queue.
     *
     * @param index The start index to begin reading from
     * @return The current instance of {@link ChronicleReader}
     */
    public ChronicleReader withStartIndex(final long index) {
        this.startIndex = index;
        return this;
    }

    /**
     * Enables tailing mode, allowing the reader to continuously read new entries as they are added to the queue.
     *
     * @return The current instance of {@link ChronicleReader}
     */
    public ChronicleReader tail() {
        this.tailInputSource = true;
        return this;
    }

    /**
     * Sets the maximum number of history records to read from the queue.
     *
     * @param maxHistoryRecords The maximum number of history records to process
     * @return The current instance of {@link ChronicleReader}
     */
    public ChronicleReader historyRecords(final long maxHistoryRecords) {
        this.maxHistoryRecords = maxHistoryRecords;
        return this;
    }

    /**
     * Sets the method reader interface for reading queue entries.
     * <p>If the provided interface name is empty, it uses a dummy handler; otherwise, it loads the class specified by the methodReaderInterface parameter.
     *
     * @param methodReaderInterface The fully qualified class name of the method reader interface
     * @return The current instance of {@link ChronicleReader}
     */
    public ChronicleReader asMethodReader(@NotNull String methodReaderInterface) {
        if (methodReaderInterface.isEmpty()) {
            entryHandlerFactory = () -> new InternalDummyMethodReaderQueueEntryHandler(wireType);
        } else try {
            this.methodReaderInterface = Class.forName(methodReaderInterface);
        } catch (ClassNotFoundException e) {
            throw Jvm.rethrow(e);
        }
        return this;
    }

    /**
     * Enables or disables showing message history in the reader.
     *
     * @param showMessageHistory {@code true} to show message history, {@code false} otherwise
     * @return The current instance of {@link ChronicleReader}
     */
    @Override
    public ChronicleReader showMessageHistory(boolean showMessageHistory) {
        this.showMessageHistory = showMessageHistory;
        return this;
    }

    /**
     * Configures a binary search comparator for the reader.
     * <p>This method dynamically loads a binary search class and allows it to configure itself by passing the current {@link ChronicleReader} instance.
     *
     * @param binarySearchClass The fully qualified class name of the binary search comparator
     * @return The current instance of {@link ChronicleReader}
     */
    @Override
    public ChronicleReader withBinarySearch(@NotNull String binarySearchClass) {
        try {
            Class<?> clazz = Class.forName(binarySearchClass);
            this.binarySearch = (BinarySearchComparator) clazz.getDeclaredConstructor().newInstance();
            // allow binary search to configure itself
            this.binarySearch.accept(this);
        } catch (Exception e) {
            throw Jvm.rethrow(e);
        }
        return this;
    }

    /**
     * Sets a content-based limiter for the reader to control how many entries can be read based on their content.
     *
     * @param contentBasedLimiter The {@link ContentBasedLimiter} to be used
     * @return The current instance of {@link ChronicleReader}
     */
    @Override
    public ChronicleReader withContentBasedLimiter(ContentBasedLimiter contentBasedLimiter) {
        this.contentBasedLimiter = contentBasedLimiter;
        return this;
    }

    /**
     * Sets an argument to be passed to the reader, typically used for custom plugin configurations.
     *
     * @param arg The argument as a string
     * @return The current instance of {@link ChronicleReader}
     */
    @Override
    public ChronicleReader withArg(@NotNull String arg) {
        this.arg = arg;
        return this;
    }

    /**
     * Sets an argument for the content-based limiter, allowing further customization of the limiter's behavior.
     *
     * @param limiterArg The argument for the limiter
     * @return The current instance of {@link ChronicleReader}
     */
    @Override
    public ChronicleReader withLimiterArg(@NotNull String limiterArg) {
        this.limiterArg = limiterArg;
        return this;
    }

    /**
     * Configures the wire type for the reader, determining how entries are serialized/deserialized.
     *
     * @param wireType The {@link WireType} to be used
     * @return The current instance of {@link ChronicleReader}
     */
    public ChronicleReader withWireType(@NotNull WireType wireType) {
        this.wireType = wireType;
        return this;
    }

    /**
     * Sets the reader to operate in reverse order, allowing it to read entries from the end of the queue backwards.
     *
     * @return The current instance of {@link ChronicleReader}
     */
    public ChronicleReader inReverseOrder() {
        this.tailerDirection = TailerDirection.BACKWARD;
        return this;
    }

    /**
     * Disables displaying the index of each queue entry during reading.
     *
     * @return The current instance of {@link ChronicleReader}
     */
    public ChronicleReader suppressDisplayIndex() {
        this.displayIndex = false;
        return this;
    }

    /**
     * Returns the argument passed to the reader.
     *
     * @return The current argument as a string
     */
    @Override
    public String arg() {
        return arg;
    }

    /**
     * Returns the argument passed to the content-based limiter.
     *
     * @return The limiter argument as a string
     */
    @Override
    public String limiterArg() {
        return limiterArg;
    }

    /**
     * Returns the class used for the method reader interface.
     *
     * @return The method reader interface class
     */
    @Override
    public Class<?> methodReaderInterface() {
        return methodReaderInterface;
    }

    /**
     * Sets the polling method used to retrieve documents from the tailer. This is mainly used for testing purposes.
     *
     * @param pollMethod The polling function to use
     * @return The current instance of {@link ChronicleReader}
     */
    public ChronicleReader withDocumentPollMethod(final Function<ExcerptTailer, DocumentContext> pollMethod) {
        this.pollMethod = pollMethod;
        return this;
    }

    /**
     * Sets the ID for the tailer to use when reading from the queue. This ID can be used to read from a specific named tailer.
     *
     * @param tailerId The tailer ID
     * @return The current instance of {@link ChronicleReader}
     */
    public ChronicleReader withTailerId(String tailerId) {
        this.tailerId = tailerId;
        return this;
    }

    /**
     * Determines whether the queue has been modified since the last check by comparing the current tail index with the last observed index.
     *
     * @param lastObservedTailIndex The index of the last observed tail
     * @param tailer                The {@link ExcerptTailer} used to read from the queue
     * @return {@code true} if the queue has been modified, {@code false} otherwise
     */
    private boolean queueHasBeenModifiedSinceLastCheck(final long lastObservedTailIndex, ExcerptTailer tailer) {
        long currentTailIndex = indexOfEnd(tailer);
        return currentTailIndex > lastObservedTailIndex;
    }

    /**
     * Moves the tailer to the specified position, taking into account whether it is the first iteration or if binary search is being used.
     *
     * @param ic              The {@link ChronicleQueue} instance
     * @param tailer          The {@link ExcerptTailer} used for reading
     * @param isFirstIteration Whether this is the first iteration of reading
     */
    private void moveToSpecifiedPosition(final ChronicleQueue ic, final ExcerptTailer tailer, final boolean isFirstIteration) {
        if (isFirstIteration) {

            // Set the direction, if we're reading backwards, start at the end by default
            tailer.direction(tailerDirection);
            if (tailerDirection == BACKWARD) {
                tailer.toEnd();
            }

            if (isSet(startIndex)) {
                tryMoveToIndex(ic, tailer);
            } else if (binarySearch != null) {
                seekBinarySearch(tailer);
            }

            if (tailerDirection == FORWARD) {
                moveTailerToEnd(tailer);
            }
        }
    }

    /**
     * Moves the {@link ExcerptTailer} to the end of the queue.
     * <p>If {@code maxHistoryRecords} is set, it moves the tailer to a specific number of entries from the end.
     * Otherwise, if tailing is enabled, it simply moves the tailer to the end.
     *
     * @param tailer The {@link ExcerptTailer} to move
     */
    private void moveTailerToEnd(ExcerptTailer tailer) {
        if (isSet(maxHistoryRecords)) {
            tailer.toEnd();
            moveToIndexNFromTheEnd(tailer, maxHistoryRecords);
        } else if (tailInputSource) {
            tailer.toEnd();
        }
    }

    /**
     * Attempts to move the {@link ExcerptTailer} to the specified start index, throwing an exception if the index is out of bounds.
     *
     * @param ic     The {@link ChronicleQueue} instance
     * @param tailer The {@link ExcerptTailer} to move
     */
    private void tryMoveToIndex(ChronicleQueue ic, ExcerptTailer tailer) {
        if (startIndex < ic.firstIndex()) {
            throw new IllegalArgumentException(String.format("startIndex 0x%xd is less than first index 0x%xd",
                    startIndex, ic.firstIndex()));
        }

        if (tailerDirection == BACKWARD && startIndex > ic.lastIndex()) {
            throw new IllegalArgumentException(String.format("startIndex 0x%xd is greater than last index 0x%xd",
                    startIndex, ic.lastIndex()));
        }

        boolean firstTime = true;
        while (!tailer.moveToIndex(startIndex)) {
            if (firstTime) {
                messageSink.accept("Waiting for startIndex " + Long.toHexString(startIndex));
                firstTime = false;
            }
            Jvm.pause(100);
        }
    }

    /**
     * Performs a binary search using the {@link BinarySearchComparator} to find the desired entry, adjusting the tailer based on the search result.
     *
     * @param tailer The {@link ExcerptTailer} to move
     */
    private void seekBinarySearch(ExcerptTailer tailer) {
        TailerDirection originalDirection = tailer.direction();
        tailer.direction(FORWARD);
        final Wire key = binarySearch.wireKey();
        long rv = BinarySearch.search(tailer, key, binarySearch);
        if (rv == -1) {
            tailer.toStart();
        } else if (rv < 0) {
            scanToFirstEntryFollowingMatch(tailer, key, -rv);
        } else {
            scanToFirstMatchingEntry(tailer, key, rv);
        }
        tailer.direction(originalDirection);
    }

    /**
     * In the event the matched value is repeated, move to the first instance of it, taking into account traversal
     * direction
     *
     * @param tailer        The {@link net.openhft.chronicle.queue.ExcerptTailer} to move
     * @param key           The value we searched for
     * @param matchingIndex The index of a matching entry
     */
    private void scanToFirstMatchingEntry(ExcerptTailer tailer, Wire key, long matchingIndex) {
        long indexToMoveTo = matchingIndex;
        tailer.direction(tailerDirection == FORWARD ? BACKWARD : FORWARD);
        tailer.moveToIndex(indexToMoveTo);
        while (true) {
            try (DocumentContext dc = tailer.readingDocument()) {
                if (!dc.isPresent())
                    break;
                try {
                    if (binarySearch.compare(dc.wire(), key) == 0)
                        indexToMoveTo = dc.index();
                    else
                        break;
                } catch (NotComparableException e) {
                    // continue
                }
            }
        }
        tailer.moveToIndex(indexToMoveTo);
    }

    /**
     * In the event we couldn't find the specified value, move to the first entry that would
     * follow it, taking into account traversal direction
     *
     * @param tailer             The {@link net.openhft.chronicle.queue.ExcerptTailer} to move
     * @param key                The key we searched for
     * @param indexAdjacentMatch The index of an entry which would appear next to the match
     */
    private void scanToFirstEntryFollowingMatch(ExcerptTailer tailer, Wire key, long indexAdjacentMatch) {
        long indexToMoveTo = -1;
        tailer.direction(tailerDirection);
        tailer.moveToIndex(indexAdjacentMatch);
        while (true) {
            try (DocumentContext dc = tailer.readingDocument()) {
                if (!dc.isPresent())
                    break;
                try {
                    if ((tailer.direction() == TailerDirection.FORWARD && binarySearch.compare(dc.wire(), key) >= 0)
                            || (tailer.direction() == BACKWARD && binarySearch.compare(dc.wire(), key) <= 0)) {
                        indexToMoveTo = dc.index();
                        break;
                    }
                } catch (NotComparableException e) {
                    break;
                }
            }
        }
        if (indexToMoveTo >= 0) {
            tailer.moveToIndex(indexToMoveTo);
        }
    }

    /**
     * Moves the {@link ExcerptTailer} a specific number of entries from the end of the queue.
     *
     * @param tailer                The {@link ExcerptTailer} to move
     * @param numberOfEntriesFromEnd The number of entries from the end to move to
     */
    private void moveToIndexNFromTheEnd(ExcerptTailer tailer, long numberOfEntriesFromEnd) {
        tailer.direction(TailerDirection.BACKWARD).toEnd();
        for (int i = 0; i < numberOfEntriesFromEnd - 1; i++) {
            try (final DocumentContext documentContext = tailer.readingDocument()) {
                if (!documentContext.isPresent()) {
                    break;
                }
            }
        }
        tailer.direction(FORWARD);
    }

    /**
     * Returns the index of the last entry in the queue.
     *
     * @param excerptTailer The {@link ExcerptTailer} used to read the queue
     * @return The index of the last entry
     */
    private long indexOfEnd(ExcerptTailer excerptTailer) {
        return excerptTailer.toEnd().index();
    }

    /**
     * Creates a {@link ChronicleQueue} based on the configuration, throwing an exception if the base path does not exist.
     *
     * @return A configured {@link ChronicleQueue} instance
     * @throws IllegalArgumentException if the base path does not exist
     */
    @NotNull
    private ChronicleQueue createQueue() {
        if (!Files.exists(basePath)) {
            throw new IllegalArgumentException(String.format("Path '%s' does not exist (absolute path '%s')", basePath, basePath.toAbsolutePath()));
        }
        return SingleChronicleQueueBuilder
                .binary(basePath.toFile())
                .readOnly(readOnly)
                .storeFileListener(StoreFileListener.noOp())
                .build();
    }

    /**
     * Stops the reader, halting any further processing of the queue.
     */
    public void stop() {
        running = false;
    }
}
