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
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueue.SUFFIX;

public enum QueueFiles {
    ;
    private static final Logger LOGGER = LoggerFactory.getLogger(RollCycleRetriever.class);

    static <T> Optional<T> processLastQueueFile(Path queuePath, WireType wireType, long blockSize, boolean readOnly,
                                                BiFunction<Wire, SingleChronicleQueueStore, T> processor) {
        if (Files.exists(queuePath) && hasQueueFiles(queuePath)) {
            final MappedBytes mappedBytes = mappedBytes(getLastQueueFile(queuePath), blockSize, readOnly);
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
                    return Optional.of(processor.apply(wire, queueStore));


                } catch (final Throwable e) {
                    LOGGER.warn("Unable to load queue store from file {}", queuePath, e);
                }

            } finally {
                mappedBytes.release();
            }
        }
        return Optional.empty();
    }

    static void writeEOFIfNeeded(@NotNull Path queuePath, @NotNull WireType wireType, long blockSize, long timeoutMS) {
        processLastQueueFile(queuePath, wireType, blockSize, false, (w, qs) -> {
            long l = qs.writePosition();
            Bytes<?> bytes = w.bytes();
            long len = Wires.lengthOf(bytes.readVolatileInt(l));
            long eofOffset = l + len + 4L;
            if (0 == bytes.readVolatileInt(eofOffset)) {
                // no EOF found - write EOF
                try {
                    bytes.writePosition(eofOffset);
                    w.writeEndOfWire(timeoutMS, TimeUnit.MILLISECONDS, eofOffset);
                } catch (TimeoutException e) {
                    Jvm.warn().on(RollCycleRetriever.class, "Timeout writing EOF for last file in " + queuePath);
                }
            }
            return null;
        });
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
