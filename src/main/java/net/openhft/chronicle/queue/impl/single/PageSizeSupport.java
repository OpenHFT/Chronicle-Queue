package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PageSizeSupport {

    private static final Pattern PAGE_SIZE_PATTERN = Pattern.compile("pagesize=[0-9]+(M|G)");
    private static final String PAGE_SIZE_PREFIX = "pagesize=";
    private static final Pattern INTEGER_PATTERN = Pattern.compile("[0-9]+");

    private static final String MOUNT_COMMAND = "mount -l";

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
                String val = bufferedReader.lines().filter(line -> line.contains(mount.toString())).findFirst().get();
                Matcher matcher = PAGE_SIZE_PATTERN.matcher(val);
                if (matcher.find()) {
                    String group = matcher.group();
                    String trimmed = group.replace(PAGE_SIZE_PREFIX, "");
                    if (trimmed.endsWith("M")) {
                        int i = parseInt(trimmed);
                        Jvm.warn().on(PageSizeSupport.class, "Determined page size of " + trimmed + " for " + mount);
                        return i * 1024 * 1024;
                    } else if (trimmed.endsWith("G")) {
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
        return OS.pageSize();
    }

    private static int parseInt(String trimmed) {
        Matcher m = INTEGER_PATTERN.matcher(trimmed);
        if (m.find()) {
            String g = m.group();
            return Integer.parseInt(g);
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
