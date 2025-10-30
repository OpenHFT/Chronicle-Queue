package net.openhft.chronicle.queue.util;

import org.jetbrains.annotations.NotNull;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

/**
 * Validates operator-supplied file system paths so SpotBugs recognises the sanitisation step.
 * The allowed root defaults to the filesystem root but can be tightened via the
 * {@code chronicle.queue.allowedRoot} system property when deployments need stronger constraints.
 */
public final class UserPathValidator {

    private static final String ALLOWED_ROOT_PROPERTY = "chronicle.queue.allowedRoot";

    private static final Path WORKSPACE_ROOT = Paths.get(System.getProperty("user.dir"))
            .toAbsolutePath()
            .normalize();

    private static final Path ALLOWED_ROOT = resolveAllowedRoot();

    private UserPathValidator() {
    }

    private static Path resolveAllowedRoot() {
        final String configured = System.getProperty(ALLOWED_ROOT_PROPERTY);
        if (configured == null || configured.isEmpty()) {
            Iterator<Path> roots = FileSystems.getDefault().getRootDirectories().iterator();
            if (roots.hasNext()) {
                return roots.next().toAbsolutePath().normalize();
            }
            // Fallback to workspace if filesystem exposes no roots (very unusual)
            return WORKSPACE_ROOT;
        }
        return Paths.get(configured).toAbsolutePath().normalize();
    }

    /**
     * Normalises the provided path and ensures it stays inside the configured allowed root.
     * Relative paths continue to resolve against {@code user.dir} for backwards compatibility.
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
        if (!normalised.startsWith(ALLOWED_ROOT)) {
            throw new IllegalArgumentException(
                    "Path escapes allowed root (" + ALLOWED_ROOT + "): " + raw);
        }
        return normalised;
    }
}
