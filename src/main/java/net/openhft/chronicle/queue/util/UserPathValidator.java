package net.openhft.chronicle.queue.util;

import org.jetbrains.annotations.NotNull;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Validates operator-supplied file system paths so SpotBugs recognises the sanitisation step.
 * The current sandbox restricts queue tooling to the working directory tree; tighten or relax
 * this in CQ-QUALITY-2173 when a broader policy is available.
 */
public final class UserPathValidator {

    private static final Path WORKSPACE_ROOT = Paths.get(System.getProperty("user.dir"))
            .toAbsolutePath()
            .normalize();

    private UserPathValidator() {
    }

    /**
     * Normalises the provided path and ensures it stays inside {@code user.dir}.
     *
     * @param raw user-provided path string
     * @return absolute, normalised {@link Path}
     */
    public static Path requireSafePath(@NotNull String raw) {
        Path candidate = Paths.get(raw);
        if (!candidate.isAbsolute()) {
            candidate = WORKSPACE_ROOT.resolve(candidate);
        }
        Path normalised = candidate.normalize();
        if (!normalised.startsWith(WORKSPACE_ROOT)) {
            throw new IllegalArgumentException("Path escapes workspace: " + raw);
        }
        return normalised;
    }
}
