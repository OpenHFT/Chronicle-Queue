/*
 * Copyright 2014-2017 Higher Frequency Trading
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
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueue.SUFFIX;

enum QueueFiles {
    ;
    private static final Logger LOGGER = LoggerFactory.getLogger(RollCycleRetriever.class);

    static <T> Optional<T> processQueueFile(Path filePath, WireType wireType, long blockSize, boolean readOnly,
                                            BiFunction<Wire, SingleChronicleQueueStore, T> processor) {
        final MappedBytes mappedBytes = mappedBytes(filePath, blockSize, readOnly);
        mappedBytes.reserve();
        try {
            final Wire wire = wireType.apply(mappedBytes);
            // move past header

            final Bytes<?> bytes = wire.bytes();
            bytes.readLimit(bytes.capacity());

            if (bytes.readLimit() < 4) {
                return Optional.empty();
            }
            final File file = mappedBytes.mappedFile().file();
            for (int i = 0; i < 500 && file.length() == 0; i++) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1L));
            }
            if (file.length() < 4) {
                LOGGER.warn("Queue file exists, but is truncated, cannot determine existing roll cycle");
                return Optional.empty();
            }

            final int firstInt = bytes.peekVolatileInt();
            if (!Wires.isReady(firstInt)) {
                return Optional.empty();
            }
            bytes.readSkip(4);
            try (final SingleChronicleQueueStore queueStore = SingleChronicleQueueBuilder.loadStore(wire)) {
                if (queueStore == null) {
                    return Optional.empty();
                }
                return Optional.ofNullable(processor.apply(wire, queueStore));

            } catch (final Throwable e) {
                LOGGER.warn("Unable to load queue store from file {}", filePath, e);
                return Optional.empty();
            }

        } finally {
            mappedBytes.release();
        }
    }

    static <T> Optional<T> processLastQueueFile(Path queuePath, WireType wireType, long blockSize, boolean readOnly,
                                                BiFunction<Wire, SingleChronicleQueueStore, T> processor) {
        if (Files.exists(queuePath) && hasQueueFiles(queuePath)) {
            return processQueueFile(getLastQueueFile(queuePath), wireType, blockSize, readOnly, processor);
        }
        return Optional.empty();
    }

    static void writeEOFIfNeeded(@NotNull Path newFilePath, @NotNull WireType wireType, long blockSize,
                                 long timeoutMS, final Pauser pauser) {
        Path queuePath = newFilePath.getParent();
        if (Files.exists(queuePath) && hasQueueFiles(queuePath))
            getLastQueueFileButNotTheNew(queuePath, newFilePath).ifPresent(f ->
                    processQueueFile(f, wireType, blockSize, false, (w, qs) -> {
                        Bytes<?> bytes = w.bytes();
                        long writePosition = qs.writePosition();
                        if (writePosition == 0) {
                            // need to wait until something has been written (or timeout)
                            // to ensure atomicity of document context
                            final long timeoutAt = System.currentTimeMillis() + timeoutMS;

                            while (qs.writePosition() == 0 &&
                                    System.currentTimeMillis() < timeoutAt) {
                                pauser.pause();
                            }
                            pauser.reset();
                            if (qs.writePosition() == 0) {
                                // nothing has happened within queue timeout
                                // nothing more can be done at this point
                                Jvm.warn().on(QueueFiles.class, "Timed out waiting for first message in " +
                                        f + ". Recovering EOF marker.");
                                try {
                                    boolean foundData = w.readDataHeader();
                                    if (!foundData) {
                                        // we are at EOF
                                        long eofPosition = bytes.readPosition();
                                        bytes.writePosition(eofPosition);
                                        w.writeEndOfWire(timeoutMS, TimeUnit.MILLISECONDS, eofPosition);
                                    }
                                } catch (EOFException ignored) {
                                }
                                return null;
                            }
                        }

                        final int recordHeader = bytes.readVolatileInt(writePosition);
                        if (Wires.isNotComplete(recordHeader)) {
                            // can't determine the length, so don't try to write anything
                            return null;
                        }

                        long recordLength = Wires.lengthOf(recordHeader);
                        long eofOffset = writePosition + recordLength + 4L;

                        // check for incomplete header in progress
                        final long nextHeaderPosition = writePosition + 4 + recordLength;
                        final int possiblyIncompleteHeader = bytes.readVolatileInt(nextHeaderPosition);
                        if (possiblyIncompleteHeader != 0 && Wires.isNotComplete(possiblyIncompleteHeader) &&
                                !Wires.isEndOfFile(possiblyIncompleteHeader)) {
                            final long timeoutAt = System.currentTimeMillis() + timeoutMS;

                            while (Wires.isNotComplete(bytes.readVolatileInt(nextHeaderPosition)) &&
                                    System.currentTimeMillis() < timeoutAt) {
                                pauser.pause();
                            }

                            if (Wires.isNotComplete(bytes.readVolatileInt(nextHeaderPosition))) {
                                Jvm.warn().on(QueueFiles.class,
                                        "Timed out waiting for incomplete message at " + nextHeaderPosition +
                                                " in " + f + ". Not writing EOF marker.");
                            } else {
                                eofOffset += Wires.lengthOf(bytes.readVolatileInt(nextHeaderPosition)) + 4;
                            }
                        }

                        final int existingValue = bytes.readVolatileInt(eofOffset);
                        if (0 == existingValue) {
                            // no EOF found - write EOF
                            bytes.writePosition(eofOffset);
                            w.writeEndOfWire(timeoutMS, TimeUnit.MILLISECONDS, eofOffset);
                        }
                        return null;
                    }));

        pauser.reset();
    }

    private static Optional<Path> getLastQueueFileButNotTheNew(final Path queuePath, final Path newFilePath) {
        try {
            try (final Stream<Path> children = Files.list(queuePath)) {
                return children.filter(p -> p.toString().endsWith(SUFFIX) && !p.equals(newFilePath)).
                        sorted(Comparator.reverseOrder()).findFirst();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(
                    String.format("Failed to list contents of known directory %s", queuePath), e);
        }
    }

    private static Path getLastQueueFile(final Path queuePath) {
        try {
            try (final Stream<Path> children = Files.list(queuePath)) {
                return children.filter(p -> p.toString().endsWith(SUFFIX)).
                        sorted(Comparator.reverseOrder()).findFirst().orElseThrow(() ->
                        new UncheckedIOException(new IOException(
                                String.format("Expected at least one %s file in directory %s",
                                        SUFFIX, queuePath))));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(
                    String.format("Failed to list contents of known directory %s", queuePath), e);
        }
    }

    private static MappedBytes mappedBytes(@NotNull Path queueFile, long blockSize, boolean readOnly) {
        long chunkSize = OS.pageAlign(blockSize);
        long overlapSize = OS.pageAlign(blockSize / 4);
        try {
            return MappedBytes.mappedBytes(queueFile.toFile(), chunkSize, overlapSize, readOnly);
        } catch (FileNotFoundException e) {
            throw new UncheckedIOException(String.format("Failed to open existing file %s", queueFile), e);
        }
    }

    private static boolean hasQueueFiles(final Path queuePath) {
        try {
            try (final Stream<Path> children = Files.list(queuePath)) {
                return children.anyMatch(p -> p.toString().endsWith(SUFFIX));
            }
        } catch (IOException e) {
            return false;
        }
    }
}
