package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.WireType;

import java.nio.file.Path;
import java.util.Optional;

public enum RollCycleRetriever {
    ;

    private static final RollCycles[] ROLL_CYCLES = RollCycles.values();

    public static Optional<RollCycle> getRollCycle(final Path queuePath, final WireType wireType, final long blockSize) {
        return QueueFiles.processLastQueueFile(queuePath, wireType, blockSize, true, (w, qs) -> {
            final int rollCycleLength = qs.rollCycleLength();
            final int rollCycleIndexCount = qs.rollIndexCount();
            final int rollCycleIndexSpacing = qs.rollIndexSpacing();

            for (final RollCycle cycle : ROLL_CYCLES) {
                if (rollCycleMatches(cycle, rollCycleLength, rollCycleIndexCount, rollCycleIndexSpacing)) {
                    return cycle;
                }
            }
            return null;
        });
    }

    private static boolean rollCycleMatches(final RollCycle cycle, final int rollCycleLength,
                                            final int rollCycleIndexCount, final int rollCycleIndexSpacing) {
        return cycle.length() == rollCycleLength && cycle.defaultIndexCount() == rollCycleIndexCount &&
                cycle.defaultIndexSpacing() == rollCycleIndexSpacing;
    }
}