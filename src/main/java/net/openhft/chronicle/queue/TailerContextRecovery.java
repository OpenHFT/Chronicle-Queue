/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.MethodReader;
import org.jetbrains.annotations.NotNull;

import static java.util.Objects.requireNonNull;

/**
 * Reconstructs same-cycle context before a restarted named tailer resumes normal processing.
 * <p>
 * A named tailer can restart in the middle of a roll cycle. If earlier records in the same cycle
 * carried context required by later records, the restarted application can rebuild that context by
 * replaying the current cycle with a temporary unnamed tailer up to, but not including, the named
 * tailer's saved index.
 * <p>
 * The handlers supplied to this class must be context-only projections. They should either implement
 * only context methods or handle data methods without external side effects. This utility never
 * advances the supplied named tailer.
 */
public final class TailerContextRecovery {

    private TailerContextRecovery() {
    }

    /**
     * Replays non-metadata documents from the start of the named tailer's current cycle up to the
     * named tailer's current index.
     *
     * @param namedTailer         the restarted named tailer, positioned at its persisted resume index
     * @param contextOnlyHandlers method-reader targets that rebuild context without business side effects
     * @return a result describing whether and how far replay proceeded
     */
    @NotNull
    public static ReplayResult replayCurrentCycleContext(@NotNull ExcerptTailer namedTailer,
                                                         @NotNull Object... contextOnlyHandlers) {
        requireNonNull(namedTailer);
        requireContextHandlers(contextOnlyHandlers);

        final long resumeIndex = namedTailer.index();
        if (resumeIndex <= 0)
            return ReplayResult.noNamedPosition(resumeIndex);

        final RollCycle rollCycle = namedTailer.queue().rollCycle();
        final int cycle = rollCycle.toCycle(resumeIndex);
        if (rollCycle.toSequenceNumber(resumeIndex) == 0)
            return ReplayResult.atCycleStart(cycle, resumeIndex);

        try (ExcerptTailer replayTailer = namedTailer.queue().createTailer()) {
            if (!replayTailer.moveToCycle(cycle))
                return ReplayResult.cycleNotAvailable(cycle, resumeIndex);

            long documentsScanned = 0;
            long lastScannedIndex = -1;
            final MethodReader reader = replayTailer.methodReaderBuilder()
                    .predicate(ignored -> isBeforeResumeIndex(replayTailer, rollCycle, cycle, resumeIndex))
                    .build(contextOnlyHandlers);

            while (reader.readOne()) {
                documentsScanned++;
                lastScannedIndex = replayTailer.lastReadIndex();
            }

            return replayTailer.index() >= resumeIndex
                    ? ReplayResult.replayed(cycle, resumeIndex, documentsScanned, lastScannedIndex)
                    : ReplayResult.stoppedBeforeNamedIndex(cycle, resumeIndex, documentsScanned, lastScannedIndex);
        }
    }

    private static boolean isBeforeResumeIndex(ExcerptTailer replayTailer, RollCycle rollCycle, int cycle, long resumeIndex) {
        final long nextIndex = replayTailer.index();
        return nextIndex >= 0 && nextIndex < resumeIndex && rollCycle.toCycle(nextIndex) == cycle;
    }

    private static void requireContextHandlers(Object[] contextOnlyHandlers) {
        requireNonNull(contextOnlyHandlers);
        if (contextOnlyHandlers.length == 0)
            throw new IllegalArgumentException("At least one context-only handler is required");
        for (Object handler : contextOnlyHandlers)
            requireNonNull(handler);
    }

    /**
     * Outcome of a current-cycle context replay attempt.
     */
    public static final class ReplayResult {
        private final Status status;
        private final int cycle;
        private final long resumeIndex;
        private final long documentsScanned;
        private final long lastScannedIndex;

        private ReplayResult(Status status, int cycle, long resumeIndex, long documentsScanned, long lastScannedIndex) {
            this.status = status;
            this.cycle = cycle;
            this.resumeIndex = resumeIndex;
            this.documentsScanned = documentsScanned;
            this.lastScannedIndex = lastScannedIndex;
        }

        private static ReplayResult noNamedPosition(long resumeIndex) {
            return new ReplayResult(Status.NO_NAMED_POSITION, Integer.MIN_VALUE, resumeIndex, 0, -1);
        }

        private static ReplayResult atCycleStart(int cycle, long resumeIndex) {
            return new ReplayResult(Status.NAMED_INDEX_AT_CYCLE_START, cycle, resumeIndex, 0, -1);
        }

        private static ReplayResult cycleNotAvailable(int cycle, long resumeIndex) {
            return new ReplayResult(Status.CYCLE_NOT_AVAILABLE, cycle, resumeIndex, 0, -1);
        }

        private static ReplayResult replayed(int cycle, long resumeIndex, long documentsScanned, long lastScannedIndex) {
            return new ReplayResult(Status.REPLAYED_TO_NAMED_INDEX, cycle, resumeIndex, documentsScanned, lastScannedIndex);
        }

        private static ReplayResult stoppedBeforeNamedIndex(int cycle, long resumeIndex, long documentsScanned, long lastScannedIndex) {
            return new ReplayResult(Status.STOPPED_BEFORE_NAMED_INDEX, cycle, resumeIndex, documentsScanned, lastScannedIndex);
        }

        /**
         * @return the replay status
         */
        public Status status() {
            return status;
        }

        /**
         * @return the cycle containing the named tailer's resume index, or {@link Integer#MIN_VALUE} if unknown
         */
        public int cycle() {
            return cycle;
        }

        /**
         * @return the named tailer's resume index captured before replay began
         */
        public long resumeIndex() {
            return resumeIndex;
        }

        /**
         * @return the number of documents scanned by the context replay tailer
         */
        public long documentsScanned() {
            return documentsScanned;
        }

        /**
         * @return the last document index scanned, or {@code -1} when no document was scanned
         */
        public long lastScannedIndex() {
            return lastScannedIndex;
        }

        /**
         * @return {@code true} when replay reached the named tailer's resume point or no replay was needed
         */
        public boolean complete() {
            return status != Status.CYCLE_NOT_AVAILABLE && status != Status.STOPPED_BEFORE_NAMED_INDEX;
        }

        @Override
        public String toString() {
            return "ReplayResult{" +
                    "status=" + status +
                    ", cycle=" + cycle +
                    ", resumeIndex=" + resumeIndex +
                    ", documentsScanned=" + documentsScanned +
                    ", lastScannedIndex=" + lastScannedIndex +
                    '}';
        }
    }

    /**
     * Status for current-cycle context replay.
     */
    public enum Status {
        /**
         * The named tailer had no real committed position. Fresh and parked named tailers use index {@code 0}.
         */
        NO_NAMED_POSITION,

        /**
         * The named tailer is positioned at sequence {@code 0}, so there are no earlier same-cycle records to replay.
         */
        NAMED_INDEX_AT_CYCLE_START,

        /**
         * Replay reached the named tailer's resume index.
         */
        REPLAYED_TO_NAMED_INDEX,

        /**
         * The roll cycle containing the named tailer's resume index is not available.
         */
        CYCLE_NOT_AVAILABLE,

        /**
         * Replay stopped before reaching the named tailer's resume index.
         */
        STOPPED_BEFORE_NAMED_INDEX
    }
}
