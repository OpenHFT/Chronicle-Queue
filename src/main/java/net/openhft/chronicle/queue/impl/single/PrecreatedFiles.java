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