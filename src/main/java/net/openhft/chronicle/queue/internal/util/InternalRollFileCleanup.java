/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.util;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Roll-file retention by named-tailer position: works out which roll files a queue can remove given
 * both a "keep the last N roll-cycle numbers" floor and the committed indexes of the queue's named
 * tailers. The keep window is based on cycle numbers, not the count of roll files currently present,
 * so sparse queues with idle cycles can retain fewer than N files.
 *
 * <p>This complements {@link InternalFileUtil#removableRollFileCandidates(File) the open-file model}:
 * that one protects readers that are <em>currently running</em>; this one protects every
 * <em>registered</em> named tailer by its persisted index, so a reader that is stopped (for example a
 * gateway restarting) never loses the rolls it has not yet read. Positions come from
 * {@link SingleChronicleQueue#namedTailerIndexes()} (a read-only metadata read), so analysis never
 * opens or advances a tailer.
 *
 * <p>Positions are a snapshot: a tailer can register, or commit its first read, at any moment -
 * including between the snapshot and a subsequent deletion - and an index of {@code 0} (freshly
 * registered or parked) pins nothing. This race is inherent, so {@code keepLastCycles} is the safety
 * margin: size it to cover the latest a reader could turn up or replay from, not just the files you
 * want on disk.
 *
 * <p>Analysis is side-effect free; deletion, disk-space checks and directory walking are the
 * caller's concern (the {@code RollFileCleanupMain} CLI entry point).
 */
public final class InternalRollFileCleanup {

    private InternalRollFileCleanup() {
    }

    /**
     * The retention verdict for one queue: the removable roll files plus the cycle bookkeeping and
     * lagging-tailer diagnosis behind the decision.
     */
    public static final class Analysis {
        private final String queue;
        private final int firstCycle;
        private final int lastCycle;
        private final int keepFloor;
        private final int deleteBelow;
        private final int oldestTailerCycle;
        private final List<String> laggingTailers;
        private final List<File> removable;
        private final int retainable;
        private final NavigableMap<String, Long> tailerIndexes;

        Analysis(String queue, int firstCycle, int lastCycle, int keepFloor, int deleteBelow,
                 int oldestTailerCycle, List<String> laggingTailers, List<File> removable,
                 int retainable, NavigableMap<String, Long> tailerIndexes) {
            this.queue = queue;
            this.firstCycle = firstCycle;
            this.lastCycle = lastCycle;
            this.keepFloor = keepFloor;
            this.deleteBelow = deleteBelow;
            this.oldestTailerCycle = oldestTailerCycle;
            this.laggingTailers = laggingTailers;
            this.removable = removable;
            this.retainable = retainable;
            this.tailerIndexes = tailerIndexes;
        }

        /** @return the queue directory name. */
        public String queue() {
            return queue;
        }

        /** @return the first present cycle. */
        public int firstCycle() {
            return firstCycle;
        }

        /** @return the last (head) cycle. */
        public int lastCycle() {
            return lastCycle;
        }

        /**
         * @return the last-N-cycles floor ({@code lastCycle - keepLastCycles + 1}), expressed as a
         * roll-cycle number rather than a present-file count
         */
        public int keepFloor() {
            return keepFloor;
        }

        /** @return the effective floor; cycles below it are {@link #removable()}. */
        public int deleteBelow() {
            return deleteBelow;
        }

        /** @return the oldest cycle any named tailer is indexed to, or -1 if none. */
        public int oldestTailerCycle() {
            return oldestTailerCycle;
        }

        /** @return named tailers indexed older than {@link #keepFloor()}, pinning rolls beyond the window. */
        public List<String> laggingTailers() {
            return laggingTailers;
        }

        /** @return the roll files removable now, earliest first. */
        public List<File> removable() {
            return removable;
        }

        /** @return the count of present roll files that are retained (at or above {@link #deleteBelow()}). */
        public int retainable() {
            return retainable;
        }

        /** @return the named-tailer positions the verdict was computed from (one metadata scan). */
        public NavigableMap<String, Long> tailerIndexes() {
            return tailerIndexes;
        }

        /** @return {@code true} when a tailer is pinning rolls older than the last-N window. */
        public boolean lagWarning() {
            return !laggingTailers.isEmpty();
        }
    }

    /**
     * Analyses an already-open queue - the form used when the caller already holds it open, such as
     * the CLI's per-queue sweep.
     *
     * @param q              the open queue
     * @param keepLastCycles the minimum number of most-recent roll-cycle numbers always kept ({@code >= 1})
     * @return the retention verdict
     */
    public static Analysis analyse(SingleChronicleQueue q, int keepLastCycles) {
        if (keepLastCycles < 1)
            throw new IllegalArgumentException("keepLastCycles must be >= 1");

        final String name = new File(q.fileAbsolutePath()).getName();
        final int first = q.firstCycle();
        final int last = q.lastCycle();
        if (first > last) // empty queue
            return new Analysis(name, first, last, last, last, -1,
                    Collections.emptyList(), Collections.emptyList(), 0, Collections.emptyNavigableMap());

        // Floor 1: always keep the last N cycles.
        final int keepFloor = Math.max(first, last - keepLastCycles + 1);

        // Floor 2: a tailer indexed older than the window pulls the floor down to it. One metadata
        // scan; the snapshot is recorded on the Analysis so callers trace the positions decided on.
        final NavigableMap<String, Long> tailerIndexes = q.namedTailerIndexes();
        int oldest = -1;
        final List<String> lagging = new ArrayList<>();
        for (Map.Entry<String, Long> e : tailerIndexes.entrySet()) {
            final long index = e.getValue();
            if (index <= 0) // parked (index 0): does not pin, resumes from oldest available if it runs
                continue;
            final int cycle = q.rollCycle().toCycle(index);
            oldest = oldest == -1 ? cycle : Math.min(oldest, cycle);
            if (cycle < keepFloor)
                lagging.add(e.getKey());
        }
        final int deleteBelow = oldest == -1 ? keepFloor : Math.min(keepFloor, oldest);

        // Walk the whole cycle-number range rather than enumerating present files: an absent cycle
        // is just a failed existence check (no store is opened or mapped), so even a large span is
        // cheap.
        final List<File> removable = new ArrayList<>();
        int retainable = 0;
        for (int cycle = first; cycle <= last; cycle++) {
            final File file = q.fileForCycle(cycle);
            if (file == null)
                continue;
            if (cycle < deleteBelow)
                removable.add(file);
            else
                retainable++;
        }
        return new Analysis(name, first, last, keepFloor, deleteBelow, oldest, lagging, removable,
                retainable, tailerIndexes);
    }
}
