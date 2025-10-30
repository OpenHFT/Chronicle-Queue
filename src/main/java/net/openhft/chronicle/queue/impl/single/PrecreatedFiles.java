/*
 * Copyright 2016-2025 chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue.impl.single;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import net.openhft.chronicle.core.Jvm;

import java.io.File;
import java.nio.file.Path;

import static net.openhft.chronicle.queue.util.UserPathValidator.requireSafePath;

/**
 * Utility class for handling pre-created Chronicle Queue files. Pre-created files have a specific
 * file suffix ({@code ".precreated"}) and can be renamed to the required queue or store file name
 * when necessary.
 */
public enum PrecreatedFiles {
    INSTANCE;

    private static final String PRE_CREATED_FILE_SUFFIX = ".precreated";

    /**
     * Renames a pre-created queue file to the required queue file name.
     * <p>
     * If the pre-created file exists and the rename operation fails, a warning is logged.
     *
     * @param requiredQueueFile The queue file that the pre-created file should be renamed to.
     */
    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "Path validated via UserPathValidator.requireSafePath")
    public static void renamePreCreatedFileToRequiredFile(final File requiredQueueFile) {
        final Path requiredPath = requireSafePath(requiredQueueFile.getPath());
        final File required = requiredPath.toFile();
        final File preCreatedFile = preCreatedPath(requiredPath).toFile();
        if (preCreatedFile.exists()) {
            if (!preCreatedFile.renameTo(required)) {
                Jvm.warn().on(PrecreatedFiles.class, "Failed to rename pre-created queue file");
            }
        }
    }

    /**
     * Creates and returns a file object representing a pre-created store file for the given
     * required store file.
     *
     * @param requiredStoreFile The file for which a pre-created store file is required.
     * @return The pre-created store file object.
     */
    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "Path validated via UserPathValidator.requireSafePath")
    public static File preCreatedFileForStoreFile(final File requiredStoreFile) {
        final Path requiredPath = requireSafePath(requiredStoreFile.getPath());
        return requiredPath.resolveSibling(requiredPath.getFileName() + PRE_CREATED_FILE_SUFFIX).toFile();
    }

    /**
     * Creates and returns a file object representing a pre-created queue file for the given
     * required queue file.
     *
     * @param requiredQueueFile The file for which a pre-created queue file is required.
     * @return The pre-created queue file object.
     */
    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "Path validated via UserPathValidator.requireSafePath")
    public static File preCreatedFile(final File requiredQueueFile) {
        final Path requiredPath = requireSafePath(requiredQueueFile.getPath());
        return preCreatedPath(requiredPath).toFile();
    }

    private static Path preCreatedPath(Path requiredPath) {
        final String fileName = requiredPath.getFileName().toString();
        final String baseName = fileName.endsWith(".cq4") && fileName.length() > 4
                ? fileName.substring(0, fileName.length() - 4)
                : fileName;
        return requiredPath.resolveSibling(baseName + PRE_CREATED_FILE_SUFFIX);
    }
}
