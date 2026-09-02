/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.main;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.internal.util.InternalRollFileCleanup;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Directory-walking roll-file retention by named-tailer position. Scans a directory of Chronicle
 * queues, and for each queue reports (and optionally deletes) the roll files removable under the
 * keep-last-N and tailer-position policy of {@link InternalRollFileCleanup}. A queue that fails to
 * open or analyse is warned about and skipped, so one corrupt queue never stops retention for the
 * rest of the root. After walking every queue it checks free disk on each distinct filesystem the
 * root and the queues live on, warning where it is below a threshold.
 *
 * <p>Runs a single sweep and exits (so external periodicity can come from cron), or loops with
 * {@code --interval <seconds>} where no scheduler is available. {@code --park} is a one-shot
 * metadata action and cannot be combined with {@code --delete} or {@code --interval}. See
 * {@code RollFileCleanupMain} for the argument reference.
 *
 * <p>Exit codes: {@code 0} success, {@code 1} usage error, {@code 2} the root is not an existing
 * directory, {@code 3} a retention warning fired under {@code --fail-on-warn}.
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
        if (args.length == 0)
            throw usageError("missing <rootDir>");
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
            try {
                if ("--keep".equals(a))
                    keep = Integer.parseInt(optionValue(args, ++i, a));
                else if ("--min-free".equals(a))
                    minFree = Jvm.parseSize(optionValue(args, ++i, a));
                else if ("--interval".equals(a))
                    intervalMs = (long) (Double.parseDouble(optionValue(args, ++i, a)) * 1e3);
                else if ("--delete".equals(a))
                    delete = true;
                else if ("--park".equals(a)) {
                    final int before = park.size();
                    addNames(park, optionValue(args, ++i, a));
                    // An empty value (e.g. an unset shell variable expanding to --park "") must fail
                    // loudly, not silently degrade to a sweep the operator mistakes for a park.
                    if (park.size() == before)
                        throw usageError("--park requires at least one tailer name");
                }
                else if ("--fail-on-warn".equals(a))
                    failOnWarn = true;
                else if ("--verbose".equals(a) || "-v".equals(a))
                    verbose = true;
                else
                    throw usageError("unknown option: " + a);
            } catch (IllegalArgumentException e) {
                throw usageError("invalid value for " + a + ": " + e.getMessage());
            }
        }
        if (keep < 1)
            throw usageError("--keep must be >= 1");
        // Parking irreversibly discards a consumer's committed position, so it is a deliberate,
        // one-shot operator action: applied once, on its own - never repeated by an interval loop
        // (which would re-reset a restarted consumer every sweep) and never bundled with a delete
        // pass whose scope it silently widens.
        if (!park.isEmpty() && (delete || intervalMs > 0))
            throw usageError("--park is a one-shot action; run it on its own, then sweep with --delete separately");
        if (!root.isDirectory()) {
            System.err.println("error: root is not an existing directory: " + root);
            throw new ExitCodeException(2);
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

    /** @return the value of option {@code name}; a trailing option with no value is a usage error. */
    private static String optionValue(String[] args, int i, String name) {
        if (i >= args.length)
            throw usageError(name + " requires a value");
        return args[i];
    }

    /** Prints the message and usage to stderr; the returned exception carries exit code 1. */
    private static ExitCodeException usageError(String message) {
        System.err.println("error: " + message);
        System.err.println("usage: RollFileCleanupMain <rootDir> [--keep N] [--min-free 10G] "
                + "[--delete] [--park name,...] [--interval <secs>] [--fail-on-warn] [--verbose]");
        System.err.println("       --park is one-shot and cannot be combined with --delete or --interval");
        System.err.println("exit codes: 0 ok; 1 usage error; 2 bad root; 3 warning (with --fail-on-warn)");
        return new ExitCodeException(1);
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

    /** A replicated named tailer's position is coordinated with its sinks and cannot be parked. */
    private static boolean isReplicated(String name) {
        return name != null && name.startsWith(SingleChronicleQueue.REPLICATED_NAMED_TAILER_PREFIX);
    }

    /** Tells the operator that a replicated park was refused. */
    private static void warnRefusedPark(File queueDir, String name) {
        Jvm.warn().on(InternalRollFileCleanupMain.class,
                "refusing to park replicated named tailer " + name + " in " + queueDir.getName()
                        + " (parking would not coordinate the replication version)");
    }

    /** Adds trimmed, non-empty comma-separated names to {@code target}. */
    private static void addNames(List<String> target, String csv) {
        for (String name : csv.split(","))
            if (!name.trim().isEmpty())
                target.add(name.trim());
    }

    /** @return the operator-facing mode used in verbose scan logs. */
    private static String sweepMode(boolean delete, List<String> park) {
        if (!park.isEmpty())
            return "park";
        return delete ? "delete" : "dry-run";
    }

    /**
     * Sweeps every queue under {@code root} once; returns {@code true} if any retention warning
     * fired. Trace detail is emitted at {@link Jvm#debug() debug} level (when {@code verbose}), and
     * warnings at {@link Jvm#warn() warn} level, so both are capturable in tests. Package-private so
     * it can be driven directly, without {@code main}'s {@code System.exit}.
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
        boolean warned = false;
        boolean skippedQueue = false;
        if (verbose)
            Jvm.debug().on(InternalRollFileCleanupMain.class, "scanning " + root + " (keep=" + keep
                    + ", " + sweepMode(delete, park) + ")");
        final List<File> queues = discoverQueues(root);
        warned |= warnNestedQueueDirs(queues);
        final Set<String> parkedNames = new HashSet<>();
        for (File queueDir : queues) {
            final InternalRollFileCleanup.Analysis a;
            try (SingleChronicleQueue q = queueFactory.build(queueDir)) {
                if (!park.isEmpty()) {
                    // Preflight before mutating metadata; a failed analysis must not partially park.
                    InternalRollFileCleanup.analyse(q, keep);
                }
                warned |= parkTailers(q, queueDir, park, parkedNames);
                a = InternalRollFileCleanup.analyse(q, keep);
                if (verbose)
                    traceQueue(q, a);
            } catch (Exception e) {
                // One unopenable or corrupt queue must not stop retention for the rest of the root.
                warned = true;
                skippedQueue = true;
                Jvm.warn().on(InternalRollFileCleanupMain.class,
                        "failed to sweep queue " + queueDir + " (skipping)", e);
                continue;
            }

            int deleted = 0;
            if (delete && !a.removable().isEmpty()) {
                // The queue is closed at this point; wait for its stores (queued to the background
                // releaser on close) to be released so no file is deleted while still memory-mapped:
                // that delete would fail on Windows and defer the space reclaim on Linux.
                BackgroundResourceReleaser.releasePendingResources();
                for (File file : a.removable()) {
                    try {
                        Files.delete(file.toPath());
                        deleted++;
                        if (verbose)
                            Jvm.debug().on(InternalRollFileCleanupMain.class, "deleted " + file.getName());
                    } catch (IOException e) { // stop on first failure so later files stay untouched (ordering matters)
                        warned = true;
                        Jvm.warn().on(InternalRollFileCleanupMain.class,
                                "failed to delete " + file + " (stopping this queue): " + e);
                        break;
                    }
                }
            }
            Jvm.debug().on(InternalRollFileCleanupMain.class,
                    a.queue() + ": cycles [" + a.firstCycle() + ".." + a.lastCycle() + "] keepFloor="
                            + a.keepFloor() + " deleteBelow=" + a.deleteBelow() + " "
                            + (delete ? "deleted" : "removable") + "="
                            + (delete ? deleted : a.removable().size())
                            + " retainable=" + a.retainable());
            if (a.lagWarning()) {
                warned = true;
                Jvm.warn().on(InternalRollFileCleanupMain.class, "queue " + a.queue() + ": tailers "
                        + a.laggingTailers() + " pin rolls below cycle " + a.keepFloor());
            }
        }

        // A park name not found in any successfully swept queue must not be silently ignored: the
        // operator believes the tailer was retired while its stale index may still pin rolls. If a
        // queue was skipped, absence cannot be proven, so do not emit a misleading typo warning.
        if (!queues.isEmpty() && !skippedQueue)
            for (String name : park)
                if (!isReplicated(name) && !parkedNames.contains(name)) {
                    warned = true;
                    Jvm.warn().on(InternalRollFileCleanupMain.class,
                            "no such named tailer " + name + " under " + root);
                }

        warned |= checkFreeDisk(root, queues, minFree, verbose);
        // A root that resolves to no queues is almost always a wrong path: warn (and so fail under
        // --fail-on-warn) rather than report a healthy sweep. The not-a-directory case is already
        // reported by discoverQueues, so only add this message for an existing directory.
        final boolean noQueues = queues.isEmpty();
        if (noQueues && root.isDirectory())
            Jvm.warn().on(InternalRollFileCleanupMain.class,
                    "no queue directories found under " + root + " - check the root path");
        warned |= noQueues;
        if (!warned)
            Jvm.debug().on(InternalRollFileCleanupMain.class, "RETENTION_OK");
        return warned;
    }

    /**
     * Applies the one-shot {@code --park} action to one queue: retire named consumers (reset to
     * index 0) so they stop pinning rolls; a restarted consumer then resumes from the oldest
     * available roll. Successfully parked names are added to {@code parkedNames} so the sweep can
     * warn about names found in no queue at all. Returns whether a replicated park was refused.
     */
    private static boolean parkTailers(SingleChronicleQueue q, File queueDir, List<String> park,
                                       Set<String> parkedNames) {
        boolean warned = false;
        for (String name : park) {
            if (isReplicated(name)) {
                warned = true;
                warnRefusedPark(queueDir, name);
            } else if (q.parkNamedTailer(name)) {
                parkedNames.add(name);
                Jvm.debug().on(InternalRollFileCleanupMain.class,
                        "RETENTION_PARKED " + queueDir.getName() + " " + name);
            }
        }
        return warned;
    }

    /** Emits a per-queue decision trace at debug level: tailer positions, the floor, and removals. */
    private static void traceQueue(SingleChronicleQueue q, InternalRollFileCleanup.Analysis a) {
        final Class<?> cls = InternalRollFileCleanupMain.class;
        Jvm.debug().on(cls, "queue " + a.queue() + " cycles [" + a.firstCycle() + ".." + a.lastCycle() + "]");
        // The positions the analysis actually decided on, not a fresh scan that could disagree.
        for (Map.Entry<String, Long> e : a.tailerIndexes().entrySet()) {
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

    /**
     * Warns for every distinct filesystem holding the root or a queue directory whose free space is
     * below {@code minFree}. Queue directories may be symlinks or mounts onto other filesystems than
     * the root, so checking the root alone can miss the disk that is actually filling.
     */
    private static boolean checkFreeDisk(File root, List<File> queues, long minFree, boolean verbose) {
        boolean warned = false;
        final Set<FileStore> seenFileStores = new HashSet<>();
        final List<File> dirs = new ArrayList<>();
        dirs.add(root);
        dirs.addAll(queues);
        for (File dir : dirs) {
            final FileStore fileStore;
            try {
                fileStore = Files.getFileStore(dir.toPath());
            } catch (IOException e) {
                continue; // the directory vanished mid-sweep; nothing left to measure
            }
            // Dedupe by the FileStore itself, whose platform equals() compares device identity.
            // name() is not unique: every tmpfs mount is "tmpfs", overlays "overlay", so keying by
            // name would silently skip a distinct same-named filesystem that is actually filling.
            if (!seenFileStores.add(fileStore))
                continue;
            final long free = dir.getUsableSpace();
            if (verbose)
                Jvm.debug().on(InternalRollFileCleanupMain.class, "free disk at " + dir + " = "
                        + Jvm.formatSize(free) + " (threshold " + Jvm.formatSize(minFree) + ")");
            if (free < minFree) {
                warned = true;
                Jvm.warn().on(InternalRollFileCleanupMain.class, "free disk " + Jvm.formatSize(free)
                        + " below threshold " + Jvm.formatSize(minFree) + " at " + dir);
            }
        }
        return warned;
    }

    /**
     * Warns when a discovered queue directory itself contains a directory that looks like a queue.
     * Directories inside a queue are not supported; the usual cause is a stray roll file copied into
     * the parent, which makes it discoverable as a queue and hides the real queues below it.
     */
    private static boolean warnNestedQueueDirs(List<File> queues) {
        boolean warned = false;
        for (File queueDir : queues) {
            final File[] subs = queueDir.listFiles(File::isDirectory);
            if (subs == null)
                continue;
            for (File sub : subs)
                if (isQueueDir(sub)) {
                    warned = true;
                    Jvm.warn().on(InternalRollFileCleanupMain.class,
                            "queue directory " + queueDir + " contains a nested queue directory "
                                    + sub.getName() + " - directories inside queues are not supported");
                }
        }
        return warned;
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
        final File[] files = dir.listFiles((d, name) -> name.endsWith(SingleChronicleQueue.SUFFIX)
                || name.equals(SingleChronicleQueue.QUEUE_METADATA_FILE));
        return files != null && files.length > 0;
    }
}
