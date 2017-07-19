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

package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.TextWire;
import org.jetbrains.annotations.NotNull;

import java.nio.file.Path;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;

final class ChronicleReader {
    private static final long UNSET_VALUE = Long.MIN_VALUE;

    private Path basePath;
    private Pattern inclusionRegex = null;
    private Pattern exclusionRegex = null;
    private long startIndex = UNSET_VALUE;
    private boolean tailInputSource = false;
    private long maxHistoryRecords = UNSET_VALUE;
    private Consumer<String> messageSink;
    private Function<ExcerptTailer, DocumentContext> pollMethod = ExcerptTailer::readingDocument;

    void execute() {
        long lastObservedTailIndex;
        long highestReachedIndex = 0L;
        do {
            lastObservedTailIndex = getCurrentTailIndex();
            final SingleChronicleQueue queue = createQueue();
            final ExcerptTailer tailer = queue.createTailer();

            if (highestReachedIndex != 0L) {
                tailer.moveToIndex(highestReachedIndex);
            }
            final Bytes textConversionTarget = Bytes.elasticByteBuffer();
            try {
                moveToSpecifiedPosition(queue, tailer);

                //noinspection InfiniteLoopStatement
                while (!Thread.currentThread().isInterrupted()) {
                    try (DocumentContext dc = pollMethod.apply(tailer)) {
                        if (!dc.isPresent()) {
                            break;
                        }

                        final Bytes<?> serialisedMessage = dc.wire().bytes();
                        final byte dataFormatIndicator = serialisedMessage.readByte(serialisedMessage.readPosition());
                        String text;

                        if (isBinaryFormat(dataFormatIndicator)) {
                            textConversionTarget.clear();
                            final BinaryWire binaryWire = new BinaryWire(serialisedMessage);
                            binaryWire.copyTo(new TextWire(textConversionTarget));
                            text = textConversionTarget.toString();
                        } else {
                            text = serialisedMessage.toString();
                        }

                        applyFiltersAndLog(text, tailer.index());
                    }
                }
            } finally {
                textConversionTarget.release();
                highestReachedIndex = tailer.index();
            }

        } while (queueHasBeenModifiedSinceLastCheck(lastObservedTailIndex) || tailInputSource);
    }

    ChronicleReader withMessageSink(final Consumer<String> messageSink) {
        this.messageSink = messageSink;
        return this;
    }

    ChronicleReader withBasePath(final Path path) {
        this.basePath = path;
        return this;
    }

    ChronicleReader withInclusionRegex(final String regex) {
        this.inclusionRegex = Pattern.compile(regex);
        return this;
    }

    ChronicleReader withExclusionRegex(final String regex) {
        this.exclusionRegex = Pattern.compile(regex);
        return this;
    }

    ChronicleReader withStartIndex(final long index) {
        this.startIndex = index;
        return this;
    }

    ChronicleReader tail() {
        this.tailInputSource = true;
        return this;
    }

    ChronicleReader historyRecords(final long maxHistoryRecords) {
        this.maxHistoryRecords = maxHistoryRecords;
        return this;
    }

    // visible for testing
    ChronicleReader withDocumentPollMethod(final Function<ExcerptTailer, DocumentContext> pollMethod) {
        this.pollMethod = pollMethod;
        return this;
    }

    private boolean queueHasBeenModifiedSinceLastCheck(final long lastObservedTailIndex) {
        return getCurrentTailIndex() != lastObservedTailIndex;
    }

    private void moveToSpecifiedPosition(final ChronicleQueue ic, final ExcerptTailer tailer) {
        if (isSet(startIndex)) {
            if (startIndex < ic.firstIndex()) {
                throw new IllegalArgumentException(String.format("startIndex %d is less than first index %d",
                        startIndex, ic.firstIndex()));
            }

            messageSink.accept("Waiting for startIndex " + startIndex);
            for (; ; ) {
                if (tailer.moveToIndex(startIndex))
                    break;
                Jvm.pause(100);
            }
        }

        if (isSet(maxHistoryRecords)) {
            tailer.toEnd();
            tailer.moveToIndex(Math.max(ic.firstIndex(), tailer.index() - maxHistoryRecords));
        } else if (tailInputSource) {
            tailer.toEnd();
        }
    }

    private long getCurrentTailIndex() {
        try (final SingleChronicleQueue queue = createQueue()) {
            return queue.createTailer().toEnd().index();
        }
    }

    @NotNull
    private SingleChronicleQueue createQueue() {
        return SingleChronicleQueueBuilder.
                binary(basePath.toFile()).
                readOnly(!OS.isWindows()).
                build();
    }

    private void applyFiltersAndLog(final String text, final long index) {
        if (inclusionRegex == null || inclusionRegex.matcher(text).find()) {
            if (exclusionRegex == null || !exclusionRegex.matcher(text).find()) {
                messageSink.accept("0x" + Long.toHexString(index) + ": ");
                messageSink.accept(text);
            }
        }
    }

    private static boolean isSet(final long configValue) {
        return configValue != UNSET_VALUE;
    }

    private static boolean isBinaryFormat(final byte dataFormatIndicator) {
        return dataFormatIndicator < 0;
    }
}