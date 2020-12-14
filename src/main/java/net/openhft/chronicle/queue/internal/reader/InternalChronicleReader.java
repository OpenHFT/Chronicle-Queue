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

package net.openhft.chronicle.queue.internal.reader;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.reader.*;
import net.openhft.chronicle.queue.util.ToolsUtil;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static net.openhft.chronicle.core.util.ObjectUtils.requireNonNull;
import static net.openhft.chronicle.queue.impl.StoreFileListener.NO_OP;

public final class InternalChronicleReader implements Reader{
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
    private Consumer<? super String> messageSink;
    private Function<ExcerptTailer, DocumentContext> pollMethod = ExcerptTailer::readingDocument;
    private WireType wireType = WireType.TEXT;
    private Supplier<QueueEntryHandler> entryHandlerFactory = () -> QueueEntryHandler.messageToText(wireType);
    private boolean displayIndex = true;
    private Class<?> methodReaderInterface;
    private volatile boolean running = true;

    static {
        ToolsUtil.warnIfResourceTracing();
    }


    private ThreadLocal<ExcerptTailer> tlTailer;

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

                tlTailer = ThreadLocal.withInitial(queue::createTailer);

                do {
                    if (highestReachedIndex != 0L) {
                        tailer.moveToIndex(highestReachedIndex);
                    }
                    final Bytes textConversionTarget = Bytes.elasticByteBuffer();
                    try {
                        moveToSpecifiedPosition(queue, tailer, isFirstIteration);
                        lastObservedTailIndex = tailer.index();
                        Consumer<String> messageConsumer = text -> applyFiltersAndLog(text, tailer.index());
                        BooleanSupplier readOne;
                        if (methodReaderInterface == null) {
                            readOne = () -> readOne(messageConverter, tailer, messageConsumer);
                        } else {
                            Bytes<ByteBuffer> bytes = Bytes.elasticHeapByteBuffer(256);
                            Object writer = WireType.TEXT.apply(bytes).methodWriter(methodReaderInterface);
                            MethodReader methodReader = tailer.methodReader(writer);
                            readOne = () -> {
                                boolean found = methodReader.readOne();
                                if (found)
                                    messageConsumer.accept(bytes.toString());
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
                            }
                            pauser.reset();
                        }
                    } finally {
                        textConversionTarget.releaseLast();
                        highestReachedIndex = tailer.index();
                        isFirstIteration = false;
                    }
                    queueHasBeenModified = queueHasBeenModifiedSinceLastCheck(lastObservedTailIndex, queue);
                    retryLastOperation = false;
                    if (!running)
                        return;
                } while (tailInputSource || queueHasBeenModified);
            } catch (final RuntimeException e) {
                if (e.getCause() instanceof DateTimeParseException) {
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

    public boolean readOne(@NotNull QueueEntryHandler messageConverter, @NotNull ExcerptTailer tailer, @NotNull Consumer<String> messageConsumer) {
        requireNonNull(messageConsumer);
        requireNonNull(tailer);
        requireNonNull(messageConsumer);
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

    InternalChronicleReader withReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
        return this;
    }

    public InternalChronicleReader withMessageSink(@NotNull Consumer<? super String> messageSink) {
        this.messageSink = requireNonNull(messageSink);
        return this;
    }

    public Consumer<? super String> messageSink() {
        return messageSink;
    }

    public InternalChronicleReader withBasePath(final @NotNull Path path) {
        this.basePath = requireNonNull(path);
        return this;
    }

    public InternalChronicleReader withInclusionRegex(final @NotNull String regex) {
        this.inclusionRegex.add(Pattern.compile(requireNonNull(regex)));
        return this;
    }

    public InternalChronicleReader withExclusionRegex(final @NotNull String regex) {
        this.exclusionRegex.add(Pattern.compile(requireNonNull(regex)));
        return this;
    }

    public InternalChronicleReader withCustomPlugin(final @NotNull ChronicleReaderPlugin customPlugin) {
        this.customPlugin = requireNonNull(customPlugin);
        return this;
    }

    public InternalChronicleReader withStartIndex(final long index) {
        this.startIndex = index;
        return this;
    }

    public InternalChronicleReader tail() {
        this.tailInputSource = true;
        return this;
    }

    public InternalChronicleReader historyRecords(final long maxHistoryRecords) {
        this.maxHistoryRecords = maxHistoryRecords;
        return this;
    }

    public InternalChronicleReader asMethodReader(String methodReaderInterface) {
        if (methodReaderInterface == null)
            entryHandlerFactory = () -> QueueEntryHandler.dummy(wireType);
        else try {
            this.methodReaderInterface = Class.forName(methodReaderInterface);
        } catch (ClassNotFoundException e) {
            throw Jvm.rethrow(e);
        }
        return this;
    }

    public InternalChronicleReader withWireType(@NotNull WireType wireType) {
        this.wireType = requireNonNull(wireType);
        return this;
    }

    public InternalChronicleReader suppressDisplayIndex() {
        this.displayIndex = false;
        return this;
    }

    public void stop() {
        running = false;
    }

    // visible for testing only
    InternalChronicleReader withDocumentPollMethod(final Function<ExcerptTailer, DocumentContext> pollMethod) {
        this.pollMethod = requireNonNull(pollMethod);
        return this;
    }

    private boolean queueHasBeenModifiedSinceLastCheck(final long lastObservedTailIndex, ChronicleQueue queue) {
        long currentTailIndex = indexOfEnd(queue);
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


    private long indexOfEnd(ChronicleQueue queue) {
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