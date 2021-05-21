/*
 * Copyright 2016-2020 chronicle.software
 *
 */

package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Time;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

public class DirectoryUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(DirectoryUtils.class);

    /**
     * Beware, this can give different results depending on whether you are
     * a) running inside maven
     * b) are running in a clean directory (without a "target" dir)
     * See OS.getTarget()
     */
    @NotNull
    public static File tempDir(String name) {
        String replacedName = name.replaceAll("[\\[\\]\\s]+", "_").replace(':', '_');
        final File tmpDir = new File(OS.getTarget(), replacedName + "-" + Time.uniqueId());
        DeleteStatic.INSTANCE.add(tmpDir);

        // Log the temporary directory in OSX as it is quite obscure
        if (OS.isMacOSX()) {
            LOGGER.info("Tmp dir: {}", tmpDir);
        }

        return tmpDir;
    }

    public static void deleteDir(@NotNull String dir) {
        IOTools.deleteDirWithFiles(new File(dir));
    }

    public static void deleteDir(@NotNull File dir) {
        IOTools.deleteDirWithFiles(dir);
    }

    enum DeleteStatic {
        INSTANCE;
        final Set<File> toDeleteList = Collections.synchronizedSet(new LinkedHashSet<>());

        {
            // TODO: should not need to do this now
            Runtime.getRuntime().addShutdownHook(new Thread(
                    () -> toDeleteList.forEach(DirectoryUtils::deleteDir)
            ));
        }

        synchronized void add(File path) {
            toDeleteList.add(path);
        }
    }
}
