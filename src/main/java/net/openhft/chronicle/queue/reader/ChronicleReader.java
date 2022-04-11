/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue.reader;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.TailerDirection;
import net.openhft.chronicle.queue.impl.single.BinarySearch;
import net.openhft.chronicle.queue.impl.single.NotComparableException;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
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
import org.jetbrains.annotations.Nullable;

import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static net.openhft.chronicle.queue.TailerDirection.BACKWARD;
import static net.openhft.chronicle.queue.TailerDirection.FORWARD;
import static net.openhft.chronicle.queue.impl.StoreFileListener.NO_OP;

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

    static {
        ToolsUtil.warnIfResourceTracing();
    }

    private static boolean isSet(final long configValue) {
        return configValue != UNSET_VALUE;
    }

    private ThreadLocal<ExcerptTailer> tlTailer;

    public void execute() {
        configureContentBasedLimiter();
        long lastObservedTailIndex;
        long highestReachedIndex = 0L;
        boolean isFirstIteration = true;
        boolean retryLastOperation;
        boolean queueHasBeenModified;
        do {
            try (final ChronicleQueue queue = createQueue();
                 final ExcerptTailer tailer = queue.createTailer()) {
                MessageHistory.set(new VanillaMessageHistory());

                tlTailer = ThreadLocal.withInitial(queue::createTailer);

                MessageCountingMessageConsumer messageConsumer = new MessageCountingMessageConsumer(matchLimit, createMessageConsumers());
                QueueEntryReader queueEntryReader = createQueueEntryReader(tailer, messageConsumer);

                do {
                    if (highestReachedIndex != 0L) {
                        tailer.moveToIndex(highestReachedIndex);
                    }
                    try {
                        moveToSpecifiedPosition(queue, tailer, isFirstIteration);
                        lastObservedTailIndex = tailer.index();

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
                    } finally {
                        highestReachedIndex = tailer.index();
                        isFirstIteration = false;
                    }
                    queueHasBeenModified = queueHasBeenModifiedSinceLastCheck(lastObservedTailIndex);
                    retryLastOperation = false;
                    if (!running || messageConsumer.matchLimitReached())
                        return;
                } while (tailerDirection != BACKWARD && (tailInputSource || queueHasBeenModified));
            } catch (final RuntimeException e) {
                if (e.getCause() != null && e.getCause() instanceof DateTimeParseException) {
                    // ignore this error - due to a race condition between
                    // the reader creating a Queue (with default roll-cycle due to no files on disk)
                    // and the writer appending to the Queue with a non-default roll-cycle
                    retryLastOperation = true;
                } else {
                    throw e;
                }
            }
        } while (retryLastOperation);

    }

    /**
     * Configure the content-based limiter if it was specified
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
     * Create the chain of message consumers according to config
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

    private boolean writeToSink(long index, String text) {
        if (displayIndex)
            messageSink.accept("0x" + Long.toHexString(index) + ": ");
        messageSink.accept(text);
        return true;
    }

    public ChronicleReader withReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
        return this;
    }

    public ChronicleReader withMatchLimit(long matchLimit) {
        this.matchLimit = matchLimit;
        return this;
    }

    public ChronicleReader withMessageSink(final @NotNull Consumer<String> messageSink) {
        this.messageSink = messageSink;
        return this;
    }

    public Consumer<String> messageSink() {
        return messageSink;
    }

    public ChronicleReader withBasePath(final @NotNull Path path) {
        this.basePath = path;
        return this;
    }

    public ChronicleReader withInclusionRegex(final @NotNull String regex) {
        this.inclusionRegex.add(Pattern.compile(regex));
        return this;
    }

    public ChronicleReader withExclusionRegex(final @NotNull String regex) {
        this.exclusionRegex.add(Pattern.compile(regex));
        return this;
    }

    public ChronicleReader withCustomPlugin(final @NotNull ChronicleReaderPlugin customPlugin) {
        this.customPlugin = customPlugin;
        return this;
    }

    public ChronicleReader withStartIndex(final long index) {
        this.startIndex = index;
        return this;
    }

    public ChronicleReader tail() {
        this.tailInputSource = true;
        return this;
    }

    public ChronicleReader historyRecords(final long maxHistoryRecords) {
        this.maxHistoryRecords = maxHistoryRecords;
        return this;
    }

    public ChronicleReader asMethodReader(@Nullable String methodReaderInterface) {
        if (methodReaderInterface == null)
            entryHandlerFactory = () -> new InternalDummyMethodReaderQueueEntryHandler(wireType);
        else try {
            this.methodReaderInterface = Class.forName(methodReaderInterface);
        } catch (ClassNotFoundException e) {
            throw Jvm.rethrow(e);
        }
        return this;
    }

    @Override
    public ChronicleReader showMessageHistory(boolean showMessageHistory) {
        this.showMessageHistory = showMessageHistory;
        return this;
    }

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

    @Override
    public ChronicleReader withContentBasedLimiter(ContentBasedLimiter contentBasedLimiter) {
        this.contentBasedLimiter = contentBasedLimiter;
        return this;
    }

    @Override
    public ChronicleReader withArg(String arg) {
        this.arg = arg;
        return this;
    }

    @Override
    public ChronicleReader withLimiterArg(@NotNull String limiterArg) {
        this.limiterArg = limiterArg;
        return this;
    }

    public ChronicleReader withWireType(@NotNull WireType wireType) {
        this.wireType = wireType;
        return this;
    }

    public ChronicleReader inReverseOrder() {
        this.tailerDirection = TailerDirection.BACKWARD;
        return this;
    }

    public ChronicleReader suppressDisplayIndex() {
        this.displayIndex = false;
        return this;
    }

    @Override
    public String arg() {
        return arg;
    }

    @Override
    public String limiterArg() {
        return limiterArg;
    }

    @Override
    public Class<?> methodReaderInterface() {
        return methodReaderInterface;
    }

    // visible for testing
    public ChronicleReader withDocumentPollMethod(final Function<ExcerptTailer, DocumentContext> pollMethod) {
        this.pollMethod = pollMethod;
        return this;
    }

    private boolean queueHasBeenModifiedSinceLastCheck(final long lastObservedTailIndex) {
        long currentTailIndex = indexOfEnd();
        return currentTailIndex > lastObservedTailIndex;
    }

    private void moveToSpecifiedPosition(final ChronicleQueue ic, final ExcerptTailer tailer, final boolean isFirstIteration) {
        if (isFirstIteration) {

            // Set the direction, if we're reading backwards, start at the end by default
            tailer.direction(tailerDirection);
            if (tailerDirection == BACKWARD) {
                tailer.toEnd();
            }

            if (isSet(startIndex)) {
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
            } else if (binarySearch != null) {
                seekBinarySearch(tailer);
            }

            if (tailerDirection == FORWARD) {
                if (isSet(maxHistoryRecords)) {
                    tailer.toEnd();
                    moveToIndexNFromTheEnd(tailer, maxHistoryRecords);
                } else if (tailInputSource) {
                    tailer.toEnd();
                }
            }
        }
    }

    private void seekBinarySearch(ExcerptTailer tailer) {
        TailerDirection originalDirection = tailer.direction();
        try {
            final Wire key = binarySearch.wireKey();
            long rv = BinarySearch.search((SingleChronicleQueue) tailer.queue(), key, binarySearch);
            if (rv == -1) {
                tailer.toStart();
            } else if (rv < 0) {
                scanToFirstEntryFollowingMatch(tailer, key, -rv);
            } else {
                scanToFirstMatchingEntry(tailer, key, rv);
            }
            tailer.direction(originalDirection);
        } catch (ParseException e) {
            throw Jvm.rethrow(e);
        }
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

    private long indexOfEnd() {
        final ExcerptTailer excerptTailer = tlTailer.get();
        long index = excerptTailer.index();
        try {
            return excerptTailer.toEnd().index();
        } finally {
            excerptTailer.moveToIndex(index);
        }
    }

    @NotNull
    private ChronicleQueue createQueue() {
        if (!Files.exists(basePath)) {
            throw new IllegalArgumentException(String.format("Path '%s' does not exist (absolute path '%s')", basePath, basePath.toAbsolutePath()));
        }
        return SingleChronicleQueueBuilder
                .binary(basePath.toFile())
                .readOnly(readOnly)
                .storeFileListener(NO_OP)
                .build();
    }

    public void stop() {
        running = false;
    }
}