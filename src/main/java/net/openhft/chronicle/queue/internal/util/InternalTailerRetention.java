/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.util;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Consumer-gated (commitment-based) roll-file retention analysis: works out which roll files a
 * queue can shed given both a "keep the last N cycles" floor and the committed positions of the
 * queue's named tailers.
 *
 * <p>This complements the liveness-based {@link InternalFileUtil#removableRollFileCandidates(File)
 * open-file model}: that one protects readers that are <em>currently running</em>; this one protects
 * every <em>registered</em> named tailer by its persisted index, so a consumer that is stopped (for
 * example a gateway restarting) never loses the rolls it has not yet read. Positions come from
 * {@link SingleChronicleQueue#namedTailerIndexes()} (a read-only metadata read), so analysis never
 * opens or advances a tailer.
 *
 * <p>Analysis is side-effect free; deletion, disk-space checks and directory walking are the
 * caller's concern (the CLI entry point and the store-file listener).
 */
public final class InternalTailerRetention {

    private InternalTailerRetention() {
    }

    /**
     * The retention verdict for one queue: the reclaimable roll files plus the cycle bookkeeping and
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

        Analysis(String queue, int firstCycle, int lastCycle, int keepFloor, int deleteBelow,
                 int oldestTailerCycle, List<String> laggingTailers, List<File> removable) {
            this.queue = queue;
            this.firstCycle = firstCycle;
            this.lastCycle = lastCycle;
            this.keepFloor = keepFloor;
            this.deleteBelow = deleteBelow;
            this.oldestTailerCycle = oldestTailerCycle;
            this.laggingTailers = laggingTailers;
            this.removable = removable;
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

        /** @return the last-N-cycles floor ({@code lastCycle - keepLastCycles + 1}). */
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

        /** @return the roll files reclaimable now, earliest first. */
        public List<File> removable() {
            return removable;
        }

        /** @return {@code true} when a tailer is pinning rolls older than the last-N window. */
        public boolean lagWarning() {
            return !laggingTailers.isEmpty();
        }
    }

    /**
     * Computes only the reclaim floor (the lowest cycle that must be kept) for an open queue: the
     * minimum of the last-N-cycles window and the oldest cycle any named tailer is indexed to. Rolls
     * strictly below the returned cycle are reclaimable. This does no file resolution, so it is cheap
     * and safe to call from a store-file-release callback.
     *
     * @param q              the open queue
     * @param keepLastCycles the minimum number of most-recent cycles always kept (>= 1)
     * @return the lowest cycle to keep; delete cycles strictly below it
     */
    public static int reclaimFloor(SingleChronicleQueue q, int keepLastCycles) {
        if (keepLastCycles < 1)
            throw new IllegalArgumentException("keepLastCycles must be >= 1");
        final int first = q.firstCycle();
        final int last = q.lastCycle();
        if (first > last)
            return first;
        final int keepFloor = Math.max(first, last - keepLastCycles + 1);
        int oldest = -1;
        for (Long index : q.namedTailerIndexes().values()) {
            // A tailer at index 0 (the new-tailer default, and how a parked tailer is marked) does
            // not pin: if it ever runs it resumes from the oldest available roll. Skip it.
            if (index <= 0)
                continue;
            final int cycle = q.rollCycle().toCycle(index);
            oldest = oldest == -1 ? cycle : Math.min(oldest, cycle);
        }
        return oldest == -1 ? keepFloor : Math.min(keepFloor, oldest);
    }

    /**
     * Analyses a queue directory, opening and closing it around the analysis.
     *
     * @param baseDir        the queue directory
     * @param keepLastCycles the minimum number of most-recent cycles always kept (>= 1)
     * @return the retention verdict
     */
    public static Analysis analyse(File baseDir, int keepLastCycles) {
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.single(baseDir).build()) {
            return analyse(q, keepLastCycles);
        }
    }

    /**
     * Analyses an already-open queue (the form used by an in-process store-file listener).
     *
     * @param q              the open queue
     * @param keepLastCycles the minimum number of most-recent cycles always kept (>= 1)
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
                    Collections.emptyList(), Collections.emptyList());

        // Floor 1: always keep the last N cycles.
        final int keepFloor = Math.max(first, last - keepLastCycles + 1);

        // Floor 2: a tailer indexed older than the window pulls the floor down to it.
        int oldest = -1;
        final List<String> lagging = new ArrayList<>();
        for (Map.Entry<String, Long> e : q.namedTailerIndexes().entrySet()) {
            final long index = e.getValue();
            if (index <= 0) // parked (index 0): does not pin, resumes from oldest available if it runs
                continue;
            final int cycle = q.rollCycle().toCycle(index);
            oldest = oldest == -1 ? cycle : Math.min(oldest, cycle);
            if (cycle < keepFloor)
                lagging.add(e.getKey());
        }
        final int deleteBelow = oldest == -1 ? keepFloor : Math.min(keepFloor, oldest);

        final List<File> removable = new ArrayList<>();
        for (int cycle = first; cycle < deleteBelow; cycle++) {
            final File file = q.fileForCycle(cycle);
            if (file != null && file.exists())
                removable.add(file);
        }
        return new Analysis(name, first, last, keepFloor, deleteBelow, oldest, lagging, removable);
    }
}
