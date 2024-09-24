/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
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

import net.openhft.chronicle.core.Jvm;

import java.io.File;

/**
 * Utility class for handling pre-created Chronicle Queue files. Pre-created files have a specific
 * file suffix ({@code ".precreated"}) and can be renamed to the required queue or store file name
 * when necessary.
 */
public enum PrecreatedFiles {
    ; // none

    private static final String PRE_CREATED_FILE_SUFFIX = ".precreated";

    /**
     * Renames a pre-created queue file to the required queue file name.
     * <p>
     * If the pre-created file exists and the rename operation fails, a warning is logged.
     *
     * @param requiredQueueFile The queue file that the pre-created file should be renamed to.
     */
    public static void renamePreCreatedFileToRequiredFile(final File requiredQueueFile) {
        final File preCreatedFile = preCreatedFile(requiredQueueFile);
        if (preCreatedFile.exists()) {
            if (!preCreatedFile.renameTo(requiredQueueFile)) {
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
    public static File preCreatedFileForStoreFile(final File requiredStoreFile) {
        return new File(requiredStoreFile.getParentFile(), requiredStoreFile.getName() +
                PRE_CREATED_FILE_SUFFIX);
    }

    /**
     * Creates and returns a file object representing a pre-created queue file for the given
     * required queue file.
     *
     * @param requiredQueueFile The file for which a pre-created queue file is required.
     * @return The pre-created queue file object.
     */
    public static File preCreatedFile(final File requiredQueueFile) {
        final String fileName = requiredQueueFile.getName();
        final String name = fileName.substring(0, fileName.length() - 4);
        return new File(requiredQueueFile.getParentFile(), name +
                PRE_CREATED_FILE_SUFFIX);
    }
}
