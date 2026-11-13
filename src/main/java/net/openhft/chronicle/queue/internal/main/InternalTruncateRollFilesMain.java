/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.main;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.core.OS;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/** Offline recursive maintenance; never invoked by the Queue lifecycle. */
public final class InternalTruncateRollFilesMain {
    private static final long ALIGNMENT = OS.pageSize();
    private static final long DEFAULT_MIN_AGE_MS = 300_000;

    private InternalTruncateRollFilesMain() {
    }

    /**
     * Inspects eligible files, preserving the newest cycle established from Queue metadata.
     * @param directory existing Queue directory
     * @param confirmedOffline all users/mappings closed, new opens prevented throughout apply
     * @param output paths and outcomes
     * @return number of files that could not be inspected or changed safely
     * @throws IOException if the directory cannot be safely inventoried before mutation
     */
    static int scan(Path directory, boolean confirmedOffline, PrintStream output) throws IOException {
        return scan(directory, confirmedOffline, output, DEFAULT_MIN_AGE_MS, Paths.get("/proc"));
    }

    private static int scan(Path directory, boolean confirmedOffline, PrintStream output, long minAgeMs, Path procRoot) throws IOException {
        RollFileCandidates inventory = RollFileCandidates.read(directory, procRoot);
        TreeMap<Integer, Path> files = inventory.files;
        if (files.isEmpty())
            return 0;
        Path newest = inventory.newest;
        int errors = 0;
        for (Path file : files.values()) {
            try {
                String outcome = inspect(file, newest, confirmedOffline, minAgeMs, procRoot);
                output.println(file.toAbsolutePath() + " " + outcome);
                if ("SKIPPED_NONZERO_TAIL".equals(outcome))
                    errors++;
            } catch (IOException | RuntimeException e) {
                error(output, file, e);
                errors++;
            }
        }
        return errors;
    }

    private static String inspect(Path file, Path newest, boolean confirmedOffline, long minAgeMs, Path procRoot) throws IOException {
        CycleFileIdentity identity = CycleFileIdentity.capture(file);
        long size = Files.size(file);
        if (file.equals(newest))
            return "SKIPPED_NEWEST";
        if (size == 0 || size % ALIGNMENT != 0)
            return "SKIPPED_SIZE";
        if (recentlyModified(identity.modifiedMillis(), System.currentTimeMillis(), minAgeMs))
            return "WARN_RECENTLY_MODIFIED: skipped; minimum age " + minAgeMs / 1000 + "s";
        return OfflineCycleFileTruncator.truncate(file, confirmedOffline, identity, procRoot).name();
    }

    /**
     * Continues independent directories after local errors; never follows directory symlinks.
     * @param root actual directory, not a symbolic link
     * @param confirmedOffline explicit external exclusion covering every queue being changed
     * @param output outcomes and final summary
     * @return error count; nonzero means incomplete, even if some files were trimmed
     * @throws IOException if the root is not a real directory or traversal cannot complete
     */
    public static int scanTree(Path root, boolean confirmedOffline, PrintStream output) throws IOException {
        return scanTree(root, confirmedOffline, output, DEFAULT_MIN_AGE_MS);
    }

    static int scanTree(Path root, boolean confirmedOffline, PrintStream output, long minAgeMs) throws IOException {
        return scanTree(root, confirmedOffline, output, minAgeMs, Paths.get("/proc"));
    }

    // Test seam only. Public entrypoints always inspect the actual /proc tree.
    static int scanTree(Path root, boolean confirmedOffline, PrintStream output, long minAgeMs, Path procRoot) throws IOException {
        if (minAgeMs < 0) throw new IllegalArgumentException("Minimum age must not be negative");
        CycleFileIdentity.noSymlinks(root);
        if (!Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS))
            throw new IOException("Root must be a real directory, not a symbolic link: " + root);
        Set<Path> directories = new TreeSet<>();
        int[] errors = {0};
        Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) {
                if (attributes.isSymbolicLink()) {
                    error(output, file, new IOException("REFUSED_SYMLINK"));
                    errors[0]++;
                } else if (attributes.isRegularFile() && file.toString().endsWith(SingleChronicleQueue.SUFFIX))
                    directories.add(file.getParent());
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) {
                error(output, file, exc);
                errors[0]++;
                return FileVisitResult.CONTINUE;
            }
        });
        for (Path directory : directories) {
            try {
                errors[0] += scan(directory, confirmedOffline, output, minAgeMs, procRoot);
            } catch (IOException | RuntimeException e) {
                error(output, directory, e);
                errors[0]++;
            }
        }
        output.println("SUMMARY directories=" + directories.size() + " errors=" + errors[0]
                + " status=" + (errors[0] == 0 ? "COMPLETE" : "INCOMPLETE"));
        return errors[0];
    }

    static boolean recentlyModified(long modified, long now, long minAgeMs) {
        return modified > now || now - modified < minAgeMs;
    }

    private static void error(PrintStream output, Path path, Exception e) {
        output.println(path.toAbsolutePath() + " ERROR " + e.getClass().getSimpleName() + ": " + e.getMessage());
    }

    /** Preview by default; --offline asserts external exclusion, not detected writer absence. */
    @SuppressWarnings("java:S106") // CLI results and diagnostics are an explicit stdout/stderr contract.
    public static void main(String[] args) {
        int status = run(args, System.out, System.err);
        if (status != 0)
            System.exit(status);
    }

    /** Shared CLI meanings: 0 complete, 1 incomplete/unsafe, 2 invalid arguments. */
    public static int run(String[] args, PrintStream output, PrintStream errors) {
        boolean offline = false;
        long minAgeMs = DEFAULT_MIN_AGE_MS;
        Path root;
        try {
            if (args.length < 1)
                throw new IllegalArgumentException("Missing root directory");
            boolean ageSet = false;
            for (int i = 1; i < args.length; i++) {
                if ("--offline".equals(args[i]) && !offline) offline = true;
                else if (args[i].startsWith("--min-age-seconds=") && !ageSet) {
                    String seconds = args[i].substring(18);
                    if (!seconds.matches("[0-9]+"))
                        throw new IllegalArgumentException("Invalid minimum age");
                    minAgeMs = Math.multiplyExact(Long.parseLong(seconds), 1000);
                    ageSet = true;
                } else throw new IllegalArgumentException("Unknown or repeated option: " + args[i]);
            }
            root = Paths.get(args[0]);
        } catch (IllegalArgumentException | ArithmeticException e) {
            errors.println(e.getMessage());
            errors.println("Usage: TruncateRollFilesMain ROOT [--offline] [--min-age-seconds=N]");
            return 2;
        }
        try {
            return scanTree(root, offline, output, minAgeMs) == 0 ? 0 : 1;
        } catch (IOException | RuntimeException e) {
            error(output, root, e);
            output.println("SUMMARY status=INCOMPLETE");
            return 1;
        }
    }
}
