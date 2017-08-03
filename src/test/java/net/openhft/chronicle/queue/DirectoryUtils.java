/*
 * Copyright 2016 higherfrequencytrading.com
 *
 */

package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class DirectoryUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(DirectoryUtils.class);
    private static final AtomicLong TIMESTAMPER = new AtomicLong(System.currentTimeMillis());

    @NotNull
    public static File tempDir(String name) {
        final File tmpDir = new File(OS.TARGET, name + "-" + Long.toString(TIMESTAMPER.getAndIncrement(), 36));

        DeleteStatic.INSTANCE.add(tmpDir);

        // Log the temporary directory in OSX as it is quite obscure
        if (OS.isMacOSX()) {
            LOGGER.info("Tmp dir: {}", tmpDir);
        }

        return tmpDir;
    }

    public static void deleteDir(@NotNull File dir) {
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDir(file);
                    } else if (!file.delete()) {
                        LOGGER.info("... unable to delete {}", file);
                    }
                }
            }
        }

        dir.delete();
    }

    enum DeleteStatic {
        INSTANCE;
        final Set<File> toDeleteList = Collections.synchronizedSet(new LinkedHashSet<>());

        {
            Runtime.getRuntime().addShutdownHook(new Thread(
                    () -> toDeleteList.forEach(DirectoryUtils::deleteDir)
            ));
        }

        synchronized void add(File path) {
            toDeleteList.add(path);
        }
    }
}
