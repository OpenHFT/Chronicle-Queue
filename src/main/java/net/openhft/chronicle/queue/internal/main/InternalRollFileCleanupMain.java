/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.main;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.internal.util.InternalRollFileCleanup;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Directory-walking roll-file retention by named-tailer position. Scans a directory of Chronicle
 * queues, and for each queue reports (and optionally deletes) the roll files removable under the
 * keep-last-N and tailer-position policy of {@link InternalRollFileCleanup}. After walking every
 * queue it checks free disk once and warns if it is below a threshold.
 *
 * <p>Runs a single sweep and exits (so external periodicity can come from cron), or loops with
 * {@code --interval <seconds>} where no scheduler is available. See {@code RollFileCleanupMain}
 * for the argument reference.
 */
public final class InternalRollFileCleanupMain {

    private InternalRollFileCleanupMain() {
    }

    /**
     * Entry point. See {@code RollFileCleanupMain#main(String[])} for the argument list.
     *
     * @param args the root directory followed by options
     * @throws InterruptedException if interrupted while sleeping between interval sweeps
     */
    public static void main(String[] args) throws InterruptedException {
        try {
            run(args);
        } catch (ExitCodeException e) {
            System.exit(e.exitCode());
        }
    }

    /**
     * Runs the command-line implementation without calling {@link System#exit(int)} directly.
     * Non-zero termination is reported by throwing {@link ExitCodeException}, which keeps the
     * implementation testable while {@link #main(String[])} remains a normal process entry point.
     *
     * @param args the root directory followed by options
     * @throws InterruptedException if interrupted while sleeping between interval sweeps
     */
    static void run(String[] args) throws InterruptedException {
        if (args.length == 0) {
            System.err.println("usage: RollFileCleanupMain <rootDir> [--keep N] [--min-free 10G] "
                    + "[--delete] [--park name,...] [--interval <secs>] [--fail-on-warn] [--verbose]");
            return;
        }
        final File root = new File(args[0]);
        int keep = 2;
        long minFree = 10L << 30; // 10 GiB
        long intervalMs = 0;
        boolean delete = false;
        boolean failOnWarn = false;
        boolean verbose = false;
        final List<String> park = new ArrayList<>();
        for (int i = 1; i < args.length; i++) {
            final String a = args[i];
            if ("--keep".equals(a))
                keep = Integer.parseInt(args[++i]);
            else if ("--min-free".equals(a))
                minFree = Jvm.parseSize(args[++i]);
            else if ("--interval".equals(a))
                intervalMs = (long) (Double.parseDouble(args[++i]) * 1e3);
            else if ("--delete".equals(a))
                delete = true;
            else if ("--park".equals(a))
                addNames(park, args[++i]);
            else if ("--fail-on-warn".equals(a))
                failOnWarn = true;
            else if ("--verbose".equals(a) || "-v".equals(a))
                verbose = true;
            else
                throw new IllegalArgumentException("unknown option: " + a);
        }

        boolean warned;
        do {
            warned = sweep(root, keep, minFree, delete, park, verbose);
            if (failOnWarn && warned)
                throw new ExitCodeException(3);
            if (intervalMs <= 0)
                break;
            Thread.sleep(intervalMs);
        } while (true);
    }

    /**
     * Signals the process exit code that {@link #main(String[])} should use.
     */
    static final class ExitCodeException extends RuntimeException {
        private static final long serialVersionUID = 0L;
        private final int exitCode;

        ExitCodeException(int exitCode) {
            this.exitCode = exitCode;
        }

        /** @return the process exit code. */
        int exitCode() {
            return exitCode;
        }
    }

    /**
     * Builds a queue for a queue directory. Tests provide this hook to keep a queue open across a
     * sweep and inspect post-sweep metadata without racing a second queue instance.
     */
    @FunctionalInterface
    interface QueueFactory {
        SingleChronicleQueue build(File queueDir);
    }

    /** Adds trimmed, non-empty comma-separated names to {@code target}. */
    private static void addNames(List<String> target, String csv) {
        for (String name : csv.split(","))
            if (!name.trim().isEmpty())
                target.add(name.trim());
    }

    /**
     * Sweeps every queue under {@code root} once; returns {@code true} if any lag or disk warning
     * fired. Trace detail is emitted at {@link Jvm#debug() debug} level (when {@code verbose}), and
     * lag/disk/failed-delete warnings at {@link Jvm#warn() warn} level, so both are capturable in
     * tests. Package-private so it can be driven directly, without {@code main}'s {@code System.exit}.
     */
    static boolean sweep(File root, int keep, long minFree, boolean delete, List<String> park,
                         boolean verbose) {
        return sweep(root, keep, minFree, delete, park, verbose,
                queueDir -> SingleChronicleQueueBuilder.single(queueDir).build());
    }

    /**
     * Sweeps every queue under {@code root} using the supplied {@code queueFactory}. This overload is
     * only for tests that need control over queue construction.
     */
    static boolean sweep(File root, int keep, long minFree, boolean delete, List<String> park,
                         boolean verbose, QueueFactory queueFactory) {
        boolean lag = false;
        if (verbose)
            Jvm.debug().on(InternalRollFileCleanupMain.class, "scanning " + root + " (keep=" + keep
                    + ", " + (delete ? "delete" : "dry-run") + ")");
        for (File queueDir : discoverQueues(root)) {
            try (SingleChronicleQueue q = queueFactory.build(queueDir)) {
                // Explicit operator action: retire named consumers (reset to index 0) so they stop
                // pinning rolls; a restarted consumer then resumes from the oldest available roll.
                // In dry-run mode this is only reported; no metadata is changed.
                if (delete) {
                    for (String name : park)
                        if (q.parkNamedTailer(name))
                            Jvm.debug().on(InternalRollFileCleanupMain.class,
                                    "RETENTION_PARKED " + queueDir.getName() + " " + name);
                } else if (!park.isEmpty()) {
                    final Map<String, Long> tailerIndexes = q.namedTailerIndexes();
                    for (String name : park)
                        if (tailerIndexes.containsKey(name))
                            Jvm.debug().on(InternalRollFileCleanupMain.class,
                                    "RETENTION_WOULD_PARK " + queueDir.getName() + " " + name);
                }

                final InternalRollFileCleanup.Analysis a = InternalRollFileCleanup.analyse(q, keep);
                if (verbose)
                    traceQueue(q, a);

                int deleted = 0;
                if (delete) {
                    for (File file : a.removable()) {
                        if (file.delete()) {
                            deleted++;
                            if (verbose)
                                Jvm.debug().on(InternalRollFileCleanupMain.class, "deleted " + file.getName());
                        } else { // stop on first failure so later files stay untouched (ordering matters)
                            Jvm.warn().on(InternalRollFileCleanupMain.class,
                                    "failed to delete " + file + " (stopping this queue)");
                            break;
                        }
                    }
                }
                Jvm.debug().on(InternalRollFileCleanupMain.class,
                        a.queue() + ": cycles [" + a.firstCycle() + ".." + a.lastCycle() + "] keepFloor="
                                + a.keepFloor() + " deleteBelow=" + a.deleteBelow() + " "
                                + (delete ? "deleted" : "removable") + "="
                                + (delete ? deleted : a.removable().size())
                                + " retainable=" + a.removable().size());
                if (a.lagWarning()) {
                    lag = true;
                    Jvm.warn().on(InternalRollFileCleanupMain.class, "queue " + a.queue() + ": tailers "
                            + a.laggingTailers() + " pin rolls below cycle " + a.keepFloor());
                }
            }
        }

        final long free = root.getUsableSpace();
        final boolean disk = free < minFree;
        if (verbose)
            Jvm.debug().on(InternalRollFileCleanupMain.class, "free disk at " + root + " = "
                    + Jvm.formatSize(free) + " (threshold " + Jvm.formatSize(minFree) + ")");
        if (disk)
            Jvm.warn().on(InternalRollFileCleanupMain.class, "free disk " + Jvm.formatSize(free)
                    + " below threshold " + Jvm.formatSize(minFree) + " at " + root);
        if (!lag && !disk)
            Jvm.debug().on(InternalRollFileCleanupMain.class, "RETENTION_OK");
        return lag || disk;
    }

    /** Emits a per-queue decision trace at debug level: tailer positions, the floor, and removals. */
    private static void traceQueue(SingleChronicleQueue q, InternalRollFileCleanup.Analysis a) {
        final Class<?> cls = InternalRollFileCleanupMain.class;
        Jvm.debug().on(cls, "queue " + a.queue() + " cycles [" + a.firstCycle() + ".." + a.lastCycle() + "]");
        for (Map.Entry<String, Long> e : q.namedTailerIndexes().entrySet()) {
            final long index = e.getValue();
            if (index <= 0)
                Jvm.debug().on(cls, "  tailer " + e.getKey() + " index=0 (parked, not pinning)");
            else {
                final int cycle = q.rollCycle().toCycle(index);
                Jvm.debug().on(cls, "  tailer " + e.getKey() + " index=" + index + " -> cycle " + cycle
                        + (cycle < a.keepFloor() ? " (LAGGING)" : ""));
            }
        }
        Jvm.debug().on(cls, "  keepFloor=" + a.keepFloor() + " oldestTailer=" + a.oldestTailerCycle()
                + " -> deleteBelow=" + a.deleteBelow());
        for (File file : a.removable())
            Jvm.debug().on(cls, "  removable " + file.getName());
    }

    /** The queue directories under {@code root} (or {@code root} itself if it is a queue). */
    private static List<File> discoverQueues(File root) {
        final List<File> queues = new ArrayList<>();
        if (isQueueDir(root)) {
            queues.add(root);
            return queues;
        }
        final File[] subs = root.listFiles(File::isDirectory);
        if (subs == null) {
            Jvm.warn().on(InternalRollFileCleanupMain.class, "not a directory: " + root);
            return queues;
        }
        for (File sub : subs)
            if (isQueueDir(sub))
                queues.add(sub);
        return queues;
    }

    /** A directory is a queue if it holds any roll file or the queue metadata file. */
    private static boolean isQueueDir(File dir) {
        final File[] files = dir.listFiles((d, name) -> name.endsWith(".cq4") || name.equals("metadata.cq4t"));
        return files != null && files.length > 0;
    }
}
