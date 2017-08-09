package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Stream;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueue.SUFFIX;

public enum RollCycleRetriever {
    ;

    private static final Logger LOGGER = LoggerFactory.getLogger(RollCycleRetriever.class);
    private static final RollCycles[] ROLL_CYCLES = RollCycles.values();

    public static Optional<RollCycle> getRollCycle(final Path queuePath, final WireType wireType, final long blockSize) {
        if (Files.exists(queuePath) && hasQueueFiles(queuePath)) {
            final MappedBytes mappedBytes = mappedBytes(getLastQueueFile(queuePath), blockSize);
            mappedBytes.reserve();
            try {
                final Wire wire = wireType.apply(mappedBytes);
                // move past header

                final Bytes<?> bytes = wire.bytes();
                bytes.readLimit(bytes.capacity());

                if (bytes.readLimit() < 4) {
                    return Optional.empty();
                }

                bytes.readSkip(4);
                try (final SingleChronicleQueueStore queueStore = SingleChronicleQueueBuilder.loadStore(wire)) {
                    if (queueStore == null) {
                        return Optional.empty();
                    }

                    final int rollCycleLength = queueStore.rollCycleLength();
                    final int rollCycleIndexCount = queueStore.rollIndexCount();
                    final int rollCycleIndexSpacing = queueStore.rollIndexSpacing();

                    for (final RollCycle cycle : ROLL_CYCLES) {
                        if (rollCycleMatches(cycle, rollCycleLength, rollCycleIndexCount, rollCycleIndexSpacing)) {
                            return Optional.of(cycle);
                        }
                    }

                } catch (final RuntimeException e) {
                    LOGGER.warn("Unable to load queue store from path {}", queuePath, e);
                }

            } finally {
                mappedBytes.release();
            }
        }
        return Optional.empty();
    }

    private static boolean rollCycleMatches(final RollCycle cycle, final int rollCycleLength,
                                            final int rollCycleIndexCount, final int rollCycleIndexSpacing) {
        return cycle.length() == rollCycleLength && cycle.defaultIndexCount() == rollCycleIndexCount &&
                cycle.defaultIndexSpacing() == rollCycleIndexSpacing;
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

    private static MappedBytes mappedBytes(@NotNull final Path queueFile,
                                           final long blockSize) {
        long chunkSize = OS.pageAlign(blockSize);
        long overlapSize = OS.pageAlign(blockSize / 4);
        try {
            return MappedBytes.mappedBytes(queueFile.toFile(), chunkSize, overlapSize, true);
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