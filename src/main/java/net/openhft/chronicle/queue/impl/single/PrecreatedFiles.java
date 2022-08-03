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

public enum PrecreatedFiles {
    ; // none

    private static final String PRE_CREATED_FILE_SUFFIX = ".precreated";

    public static void renamePreCreatedFileToRequiredFile(final File requiredQueueFile) {
        final File preCreatedFile = preCreatedFile(requiredQueueFile);
        if (preCreatedFile.exists()) {
            if (!preCreatedFile.renameTo(requiredQueueFile)) {
                Jvm.warn().on(PrecreatedFiles.class, "Failed to rename pre-created queue file");
            }
        }
    }

    public static File preCreatedFileForStoreFile(final File requiredStoreFile) {
        return new File(requiredStoreFile.getParentFile(), requiredStoreFile.getName() +
                PRE_CREATED_FILE_SUFFIX);
    }

    public static File preCreatedFile(final File requiredQueueFile) {
        final String fileName = requiredQueueFile.getName();
        final String name = fileName.substring(0, fileName.length() - 4);
        return new File(requiredQueueFile.getParentFile(), name +
                PRE_CREATED_FILE_SUFFIX);
    }
}