/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.MethodReader;
import org.jetbrains.annotations.NotNull;

import static java.util.Objects.requireNonNull;
import static net.openhft.chronicle.queue.TailerDirection.FORWARD;

/**
 * Reconstructs same-cycle context before a restarted tailer resumes normal processing.
 * <p>
 * A named tailer can restart in the middle of a roll cycle. If earlier records in the same cycle
 * carried context required by later records, the restarted application can rebuild that context by
 * replaying the current cycle with a temporary unnamed tailer up to, but not including, the named
 * tailer's saved index.
 * <p>
 * The supplied replay handlers must collectively handle every method that may occur in the replayed
 * stream. Context methods should update only the reconstructed projection and ordinary data methods
 * must have no external side effects. This utility never advances the supplied resume tailer.
 */
public final class TailerContextRecovery {

    private TailerContextRecovery() {
    }

    /**
     * Replays non-metadata documents from the start of the resume tailer's current cycle up to the
     * resume tailer's current index.
     *
     * @param resumeTailer   a forward tailer positioned at a trusted resume index; normally a restarted named tailer
     * @param replayHandlers method-reader targets that collectively handle the replay stream without business side effects
     * @return a result describing whether and how far replay proceeded
     * @throws IllegalStateException if a replayed method has no matching handler
     */
    @NotNull
    public static ReplayResult replayCurrentCycleContext(@NotNull ExcerptTailer resumeTailer,
                                                         @NotNull Object... replayHandlers) {
        requireNonNull(resumeTailer);
        if (resumeTailer.direction() != FORWARD)
            throw new IllegalArgumentException("The resume tailer must use FORWARD direction");

        return replayCurrentCycleContext(resumeTailer.queue(), resumeTailer.index(), replayHandlers);
    }

    /**
     * Replays non-metadata documents from the start of the cycle containing {@code resumeIndex}
     * up to that index. This overload is useful when a persisted position has been obtained
     * separately, including when the queue is read-only.
     *
     * @param queue          the queue containing the records to replay
     * @param resumeIndex    a trusted resume index in {@code queue}
     * @param replayHandlers method-reader targets that collectively handle the replay stream without business side effects
     * @return a result describing whether and how far replay proceeded
     * @throws IllegalStateException if a replayed method has no matching handler
     */
    @NotNull
    public static ReplayResult replayCurrentCycleContext(@NotNull ChronicleQueue queue,
                                                         long resumeIndex,
                                                         @NotNull Object... replayHandlers) {
        requireNonNull(queue);
        requireReplayHandlers(replayHandlers);

        if (resumeIndex <= 0)
            return ReplayResult.noResumePosition(resumeIndex);

        final RollCycle rollCycle = queue.rollCycle();
        final int cycle = rollCycle.toCycle(resumeIndex);
        if (rollCycle.toSequenceNumber(resumeIndex) == 0)
            return ReplayResult.atCycleStart(cycle, resumeIndex);

        try (ExcerptTailer replayTailer = queue.createTailer()) {
            if (!replayTailer.moveToCycle(cycle))
                return ReplayResult.cycleNotAvailable(cycle, resumeIndex);

            long documentsScanned = 0;
            long lastScannedIndex = -1;
            final long[] rejectedDocumentIndex = {-1};
            final MethodReader reader = replayTailer.methodReaderBuilder()
                    .defaultParselet((methodName, valueIn) -> {
                        throw new IllegalStateException("No replay handler for method '" + methodName + "'");
                    })
                    .predicate(ignored -> isBeforeResumeIndex(replayTailer, rollCycle, cycle, resumeIndex))
                    .methodReaderInterceptorReturns((method, target, arguments, invocation) -> {
                        final long documentIndex = replayTailer.index();
                        if (!isReplayDocumentIndex(documentIndex, rollCycle, cycle, resumeIndex)) {
                            rejectedDocumentIndex[0] = documentIndex;
                            return null;
                        }
                        return invocation.invoke(method, target, arguments);
                    })
                    .build(replayHandlers);

            while (reader.readOne()) {
                documentsScanned++;
                lastScannedIndex = replayTailer.lastReadIndex();
                if (!isReplayDocumentIndex(lastScannedIndex, rollCycle, cycle, resumeIndex)) {
                    rejectedDocumentIndex[0] = lastScannedIndex;
                    break;
                }
            }

            return rejectedDocumentIndex[0] < 0 && replayTailer.index() == resumeIndex
                    ? ReplayResult.replayed(cycle, resumeIndex, documentsScanned, lastScannedIndex)
                    : ReplayResult.replayIncomplete(cycle, resumeIndex, documentsScanned, lastScannedIndex);
        }
    }

    private static boolean isBeforeResumeIndex(ExcerptTailer replayTailer, RollCycle rollCycle, int cycle, long resumeIndex) {
        final long nextIndex = replayTailer.index();
        return isReplayDocumentIndex(nextIndex, rollCycle, cycle, resumeIndex);
    }

    private static boolean isReplayDocumentIndex(long documentIndex, RollCycle rollCycle, int cycle, long resumeIndex) {
        return documentIndex >= 0 && documentIndex < resumeIndex && rollCycle.toCycle(documentIndex) == cycle;
    }

    private static void requireReplayHandlers(Object[] replayHandlers) {
        requireNonNull(replayHandlers);
        if (replayHandlers.length == 0)
            throw new IllegalArgumentException("At least one replay handler is required");
        for (Object handler : replayHandlers)
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

        private static ReplayResult noResumePosition(long resumeIndex) {
            return new ReplayResult(Status.NO_RESUME_POSITION, Integer.MIN_VALUE, resumeIndex, 0, -1);
        }

        private static ReplayResult atCycleStart(int cycle, long resumeIndex) {
            return new ReplayResult(Status.RESUME_INDEX_AT_CYCLE_START, cycle, resumeIndex, 0, -1);
        }

        private static ReplayResult cycleNotAvailable(int cycle, long resumeIndex) {
            return new ReplayResult(Status.CYCLE_NOT_AVAILABLE, cycle, resumeIndex, 0, -1);
        }

        private static ReplayResult replayed(int cycle, long resumeIndex, long documentsScanned, long lastScannedIndex) {
            return new ReplayResult(Status.REPLAYED_TO_RESUME_INDEX, cycle, resumeIndex, documentsScanned, lastScannedIndex);
        }

        private static ReplayResult replayIncomplete(int cycle, long resumeIndex, long documentsScanned, long lastScannedIndex) {
            return new ReplayResult(Status.REPLAY_INCOMPLETE, cycle, resumeIndex, documentsScanned, lastScannedIndex);
        }

        /**
         * @return the replay status
         */
        public Status status() {
            return status;
        }

        /**
         * @return the cycle containing the resume index, or {@link Integer#MIN_VALUE} if unknown
         */
        public int cycle() {
            return cycle;
        }

        /**
         * @return the trusted resume index captured before replay began
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
         * @return {@code true} when replay reached the resume point or no replay was needed
         */
        public boolean complete() {
            switch (status) {
                case NO_RESUME_POSITION:
                case RESUME_INDEX_AT_CYCLE_START:
                case REPLAYED_TO_RESUME_INDEX:
                    return true;
                default:
                    return false;
            }
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
         * The resume tailer had no real committed position. Fresh and parked named tailers use index {@code 0}.
         */
        NO_RESUME_POSITION,

        /**
         * The resume tailer is positioned at sequence {@code 0}, so there are no earlier same-cycle records to replay.
         */
        RESUME_INDEX_AT_CYCLE_START,

        /**
         * Replay reached the resume tailer's index.
         */
        REPLAYED_TO_RESUME_INDEX,

        /**
         * The roll cycle containing the resume index is not available.
         */
        CYCLE_NOT_AVAILABLE,

        /**
         * Replay did not reach the resume index exactly.
         */
        REPLAY_INCOMPLETE
    }
}
