package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * FIXME Clean up error handling + logging
 * FIXME Clean up cross platform portability
 * FIXME Consider port to core
 */
public final class PageSizeSupport {

    private static final Pattern PAGE_SIZE_PATTERN = Pattern.compile("pagesize=[0-9]+(M|G)");
    private static final String PAGE_SIZE_PREFIX = "pagesize=";
    private static final Pattern INTEGER_PATTERN = Pattern.compile("[0-9]+");

    private static final String MOUNT_COMMAND = "mount -l";

    private PageSizeSupport() {
        // Intentional no-op
    }

    public static int resolvePageSize(Path path) {
        if (OS.isLinux()) {
            return pageSizeOfMount(findMount(path));
        } else {
            return fallbackPageSize();
        }
    }

    private static int pageSizeOfMount(Path mount) {
        int fallbackPageSize = fallbackPageSize();
        Runtime runtime = Runtime.getRuntime();
        try {
            Process pr = runtime.exec(MOUNT_COMMAND);
            try (InputStreamReader inputStreamReader = new InputStreamReader(pr.getInputStream()); BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
                Optional<String> result = bufferedReader.lines().filter(line -> line.contains(mount.toString())).findFirst();
                if (!result.isPresent()) {
                    return fallbackPageSize;
                }
                String mountInfo = result.get();
                Matcher matcher = PAGE_SIZE_PATTERN.matcher(mountInfo);
                if (matcher.find()) {
                    String group = matcher.group();
                    String pageSizeString = group.replace(PAGE_SIZE_PREFIX, "");
                    if (pageSizeString.endsWith("M")) {
                        int i = parseInt(pageSizeString);
                        Jvm.warn().on(PageSizeSupport.class, "Determined page size of " + pageSizeString + " for " + mount);
                        return i * 1024 * 1024;
                    } else if (pageSizeString.endsWith("G")) {
                        throw new UnsupportedOperationException();
                    } else {
                        return fallbackPageSize;
                    }
                } else {
                    return fallbackPageSize;
                }
            }
        } catch (IOException e) {
            Jvm.warn().on(PageSizeSupport.class, "Encountered IOException", e);
            return fallbackPageSize;
        }
    }

    private static int fallbackPageSize() {
        return OS.SAFE_PAGE_SIZE;
    }

    private static int parseInt(String trimmed) {
        Matcher matcher = INTEGER_PATTERN.matcher(trimmed);
        if (matcher.find()) {
            String group = matcher.group();
            return Integer.parseInt(group);
        } else {
            Jvm.warn().on(PageSizeSupport.class, "Could not parse page size");
            return fallbackPageSize();
        }
    }

    private static Path findMount(Path path) {
        try {
            FileStore fileStore = Files.getFileStore(path);
            Path tmp = path.toAbsolutePath();
            Path mount = tmp;

            while ((tmp = tmp.getParent()) != null && fileStore.equals(Files.getFileStore(tmp))) {
                mount = tmp;
            }
            return mount;
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

}
