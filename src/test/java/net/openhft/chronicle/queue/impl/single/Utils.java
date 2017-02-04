/*
 * Copyright 2016 higherfrequencytrading.com
 *
 */

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Created by Rob Austin
 */
public class Utils {

    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

    public static File tempDir(String name) {
        final File tmpDir = new File(OS.TARGET, name + "-" + System.nanoTime());

        DeleteStatic.INSTANCE.add(tmpDir);

        // Log the temporary directory in OSX as it is quite obscure
        if (OS.isMacOSX()) {
            LOGGER.info("Tmp dir: {}", tmpDir);
        }

        return tmpDir;
    }

    static void deleteDir(File dir) {
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
                    () -> toDeleteList.forEach(Utils::deleteDir)
            ));
        }

        synchronized void add(File path) {
            toDeleteList.add(path);
        }
    }
}
