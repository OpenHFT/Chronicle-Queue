/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
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
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
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

public final class ChronicleReader {
    private static final long UNSET_VALUE = Long.MIN_VALUE;

    private final List<Pattern> inclusionRegex = new ArrayList<>();
    private final List<Pattern> exclusionRegex = new ArrayList<>();
    private final Pauser pauser = Pauser.balanced();
    private Path basePath;
    private long startIndex = UNSET_VALUE;
    private boolean tailInputSource = false;
    private long maxHistoryRecords = UNSET_VALUE;
    private boolean readOnly = true;
    private ChronicleReaderPlugin customPlugin;
    private Consumer<String> messageSink;
    private Function<ExcerptTailer, DocumentContext> pollMethod = ExcerptTailer::readingDocument;
    private WireType wireType = WireType.TEXT;
    private Supplier<QueueEntryHandler> entryHandlerFactory = () -> new MessageToTextQueueEntryHandler(wireType);
    private boolean displayIndex = true;

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

    public void execute() {
        long lastObservedTailIndex;
        long highestReachedIndex = 0L;
        boolean isFirstIteration = true;
        boolean retryLastOperation;
        boolean queueHasBeenModified;
        do {
            try (final ChronicleQueue queue = createQueue();
                 final QueueEntryHandler messageConverter = entryHandlerFactory.get()) {
                final ExcerptTailer tailer = queue.createTailer();

                do {
                    if (highestReachedIndex != 0L) {
                        tailer.moveToIndex(highestReachedIndex);
                    }
                    final Bytes textConversionTarget = Bytes.elasticByteBuffer();
                    try {
                        moveToSpecifiedPosition(queue, tailer, isFirstIteration);
                        lastObservedTailIndex = tailer.index();

                        while (!Thread.currentThread().isInterrupted()) {
                            try (DocumentContext dc = pollMethod.apply(tailer)) {
                                if (!dc.isPresent()) {
                                    if (tailInputSource) {
                                        pauser.pause();
                                    }
                                    break;
                                }
                                pauser.reset();

                                if (customPlugin == null) {
                                    messageConverter.accept(dc.wire(), text -> applyFiltersAndLog(text, tailer.index()));
                                } else {
                                    customPlugin.onReadDocument(dc);
                                }
                            }
                        }
                    } finally {
                        textConversionTarget.release();
                        highestReachedIndex = tailer.index();
                        isFirstIteration = false;
                    }
                    queueHasBeenModified = queueHasBeenModifiedSinceLastCheck(lastObservedTailIndex, queue);
                    retryLastOperation = false;
                } while (tailInputSource || queueHasBeenModified);
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

    ChronicleReader withReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
        return this;
    }

    public ChronicleReader withMessageSink(final Consumer<String> messageSink) {
        this.messageSink = messageSink;
        return this;
    }

    public Consumer<String> messageSink() {
        return messageSink;
    }

    public ChronicleReader withBasePath(final Path path) {
        this.basePath = path;
        return this;
    }

    public ChronicleReader withInclusionRegex(final String regex) {
        this.inclusionRegex.add(Pattern.compile(regex));
        return this;
    }

    public ChronicleReader withExclusionRegex(final String regex) {
        this.exclusionRegex.add(Pattern.compile(regex));
        return this;
    }

    public ChronicleReader withCustomPlugin(final ChronicleReaderPlugin customPlugin) {
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

    public ChronicleReader asMethodReader(String methodReaderInterface) {
        entryHandlerFactory = () -> new DummyMethodReaderQueueEntryHandler(wireType);
        return this;
    }

    public ChronicleReader withWireType(WireType wireType) {
        this.wireType = wireType;
        return this;
    }

    public ChronicleReader suppressDisplayIndex() {
        this.displayIndex = false;
        return this;
    }

    // visible for testing
    ChronicleReader withDocumentPollMethod(final Function<ExcerptTailer, DocumentContext> pollMethod) {
        this.pollMethod = pollMethod;
        return this;
    }

    private boolean queueHasBeenModifiedSinceLastCheck(final long lastObservedTailIndex, ChronicleQueue queue) {
        long currentTailIndex = getCurrentTailIndex(queue);
        return currentTailIndex > lastObservedTailIndex;
    }

    private void moveToSpecifiedPosition(final ChronicleQueue ic, final ExcerptTailer tailer, final boolean isFirstIteration) {
        if (isSet(startIndex) && isFirstIteration) {
            if (startIndex < ic.firstIndex()) {
                throw new IllegalArgumentException(String.format("startIndex %d is less than first index %d",
                        startIndex, ic.firstIndex()));
            }

            messageSink.accept("Waiting for startIndex " + startIndex);
            while (!tailer.moveToIndex(startIndex)) {
                Jvm.pause(100);
            }
        }

        if (isSet(maxHistoryRecords) && isFirstIteration) {
            tailer.toEnd();
            tailer.moveToIndex(Math.max(ic.firstIndex(), tailer.index() - maxHistoryRecords));
        } else if (tailInputSource && isFirstIteration) {
            tailer.toEnd();
        }
    }

    private long getCurrentTailIndex(ChronicleQueue queue) {
        return queue.createTailer().toEnd().index();
    }

    @NotNull
    private ChronicleQueue createQueue() {
        if (!Files.exists(basePath)) {
            throw new IllegalArgumentException(String.format("Path %s does not exist", basePath));
        }
        return SingleChronicleQueueBuilder
                .binary(basePath.toFile())
                .readOnly(readOnly)
                .build();
    }

    protected void applyFiltersAndLog(final String text, final long index) {
        if (inclusionRegex.isEmpty() || checkForMatches(inclusionRegex, text, true)) {
            if (exclusionRegex.isEmpty() || checkForMatches(exclusionRegex, text, false)) {
                if (displayIndex)
                    messageSink.accept("0x" + Long.toHexString(index) + ": ");
                messageSink.accept(text);
            }
        }
    }
}