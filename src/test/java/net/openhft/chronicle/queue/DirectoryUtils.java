/*
 * Copyright 2016-2020 chronicle.software
 *
 */

package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.MappedUniqueTimeProvider;
import net.openhft.chronicle.bytes.PageUtil;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.shutdown.PriorityHook;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

public class DirectoryUtils {

    /**
     * The {@link MappedUniqueTimeProvider} class will attempt to instantiate a memory mapped file under the
     * directory returned by OS.getTarget(). In the scope of hugetlbfs tests this is mapped to hugetlbfs. That class
     * is not compatible with hugetlbfs out of the box so the static block below ensures that it creates its memory
     * mapped file on another filesystem.
     */
    static {
        if (PageUtil.isHugePage(OS.getTarget())) {
            System.setProperty("timestamp.dir", System.getProperty("java.io.tmpdir"));
        }
    }

    /**
     * Beware, this can give different results depending on whether you are
     * a) running inside maven
     * b) are running in a clean directory (without a "target" dir)
     * See OS.getTarget()
     */
    @NotNull
    public static File tempDir(String name) {
        String replacedName = name.replaceAll("[\\[\\]\\s]+", "_").replace(':', '_');
        final File tmpDir = new File(OS.getTarget(), replacedName + "-" + uniqueId());
        DeleteStatic.INSTANCE.add(tmpDir);
        tmpDir.deleteOnExit();

        // Log the temporary directory in OSX as it is quite obscure
        if (OS.isMacOSX()) {
            Jvm.debug().on(DirectoryUtils.class, "Tmp dir: " + tmpDir);
        }

        return tmpDir;
    }

    public static String uniqueId() {
        long l;
        try {
            l = MappedUniqueTimeProvider.INSTANCE.currentTimeMicros();
        } catch (IllegalStateException var3) {
            l = SystemTimeProvider.INSTANCE.currentTimeMicros();
        }

        return Long.toString(l, 36);
    }

    @Deprecated(/* Remove in x.25. Use IOTools.deleteDirWithFiles */)
    public static void deleteDir(@NotNull String dir) {
        IOTools.deleteDirWithFiles(new File(dir));
    }

    @Deprecated(/* Remove in x.25. Use IOTools.deleteDirWithFiles */)
    public static void deleteDir(@NotNull File dir) {
        IOTools.deleteDirWithFiles(dir);
    }

    enum DeleteStatic {
        INSTANCE;
        final Set<File> toDeleteList = Collections.synchronizedSet(new LinkedHashSet<>());

        {
            // TODO: should not need to do this now
            PriorityHook.add(100, () -> toDeleteList.forEach(IOTools::deleteDirWithFiles));
        }

        synchronized void add(File path) {
            toDeleteList.add(path);
        }
    }
}
