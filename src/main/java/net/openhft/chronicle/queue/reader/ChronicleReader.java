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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.TailerDirection;
import net.openhft.chronicle.queue.impl.single.BinarySearch;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.reader.comparator.BinarySearchComparator;
import net.openhft.chronicle.queue.util.ToolsUtil;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.MessageHistory;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
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

    static {
        ToolsUtil.warnIfResourceTracing();
    }

    private static boolean checkForMatches(final List<Pattern> patterns, final String text,
                                           final boolean shouldBePresent) {
        for (Pattern pattern : patterns) {
            if (!shouldBePresent == pattern.matcher(text).find()) {
                return false;
            }
        }
        return true;
    }

    private static boolean isSet(final long configValue) {
        return configValue != UNSET_VALUE;
    }

    private ThreadLocal<ExcerptTailer> tlTailer;

    public void execute() {
        long lastObservedTailIndex;
        long highestReachedIndex = 0L;
        boolean isFirstIteration = true;
        boolean retryLastOperation;
        boolean queueHasBeenModified;
        final AtomicLong matchCounter = new AtomicLong(0L);
        do {
            try (final ChronicleQueue queue = createQueue();
                 final QueueEntryHandler messageConverter = entryHandlerFactory.get()) {
                final ExcerptTailer tailer = queue.createTailer();

                tlTailer = ThreadLocal.withInitial(queue::createTailer);

                do {
                    if (highestReachedIndex != 0L) {
                        tailer.moveToIndex(highestReachedIndex);
                    }
                    final Bytes textConversionTarget = Bytes.elasticByteBuffer();
                    try {
                        moveToSpecifiedPosition(queue, tailer, isFirstIteration);
                        lastObservedTailIndex = tailer.index();
                        Consumer<String> messageConsumer = text -> applyFiltersAndLog(text, tailer.index(), matchCounter);
                        BooleanSupplier readOne;
                        if (methodReaderInterface == null) {
                            readOne = () -> readOne(messageConverter, tailer, messageConsumer);
                        } else {
                            // TODO: consider unifying this with messageConverter
                            Bytes<ByteBuffer> bytes = Bytes.elasticHeapByteBuffer(256);
                            Object writer = WireType.TEXT.apply(bytes).methodWriter(methodReaderInterface);
                            MethodReader methodReader = tailer.methodReader(writer);
                            readOne = () -> {
                                boolean found = methodReader.readOne();
                                if (found) {
                                    String msg = showMessageHistory ? (MessageHistory.get().toString() + System.lineSeparator()) : "";
                                    messageConsumer.accept(msg + bytes);
                                }
                                bytes.clear();
                                return found;
                            };
                        }

                        while (!Thread.currentThread().isInterrupted()) {
                            boolean found = readOne.getAsBoolean();

                            if (!found) {
                                if (tailInputSource) {
                                    pauser.pause();
                                }
                                break;
                            } else {
                                if (matchLimitReached(matchCounter.get())) {
                                    break;
                                }
                            }
                            pauser.reset();
                        }
                    } finally {
                        textConversionTarget.releaseLast();
                        highestReachedIndex = tailer.index();
                        isFirstIteration = false;
                    }
                    queueHasBeenModified = queueHasBeenModifiedSinceLastCheck(lastObservedTailIndex);
                    retryLastOperation = false;
                    if (!running || matchLimitReached(matchCounter.get()))
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

    private boolean matchLimitReached(long matches) {
        return matchLimit > 0 && matches >= matchLimit;
    }

    public boolean readOne(@NotNull QueueEntryHandler messageConverter, @NotNull ExcerptTailer tailer, @NotNull Consumer<String> messageConsumer) {
        try (DocumentContext dc = pollMethod.apply(tailer)) {
            if (!dc.isPresent()) {
                return false;
            }

            if (customPlugin == null) {
                messageConverter.accept(dc.wire(), messageConsumer);
            } else {
                customPlugin.onReadDocument(dc, messageConsumer);
            }
        }
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
            entryHandlerFactory = () -> QueueEntryHandler.dummy(wireType);
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
    public ChronicleReader withArg(String arg) {
        this.arg = arg;
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
                startIndex = seekBinarySearch(tailer);
                tailer.moveToIndex(startIndex);
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

    private long seekBinarySearch(ExcerptTailer tailer) {
        try {
            final Wire key = binarySearch.wireKey();
            long rv = BinarySearch.search((SingleChronicleQueue) tailer.queue(), key, binarySearch);
            if (rv == -1)
                return tailer.queue().firstIndex();
            if (rv < 0) {
                // we can get an approximate match even if we search for a key after the last entry - this works around that
                tailer.moveToIndex(-rv);
                while (true) {
                    try (DocumentContext dc = tailer.readingDocument()) {
                        if (!dc.isPresent())
                            return dc.index();
                        if (binarySearch.compare(dc.wire(), key) >= 0)
                            return dc.index();
                    }
                }
            }
            return rv;
//            // if we have multiple entries that match we need to go back to the 1st
//            while (true) {
//                try (DocumentContext dc = tailer.readingDocument()) {
//                    if (!dc.isPresent())
//                        return dc.index();
//
//                }
//            }
        } catch (ParseException e) {
            throw Jvm.rethrow(e);
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

    protected void applyFiltersAndLog(final String text, final long index, AtomicLong matches) {
        if (inclusionRegex.isEmpty() || checkForMatches(inclusionRegex, text, true)) {
            if (exclusionRegex.isEmpty() || checkForMatches(exclusionRegex, text, false)) {
                matches.incrementAndGet();
                if (displayIndex)
                    messageSink.accept("0x" + Long.toHexString(index) + ": ");
                messageSink.accept(text);
            }
        }
    }

    public void stop() {
        running = false;
    }
}