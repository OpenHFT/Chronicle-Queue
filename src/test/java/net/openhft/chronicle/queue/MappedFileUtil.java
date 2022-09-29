package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public enum MappedFileUtil {
    ;
    private static final Path PROC_SELF_MAPS = Paths.get("/proc/self/maps");

    // See https://man7.org/linux/man-pages/man5/proc.5.html
    private static final Pattern LINE_PATTERN = Pattern.compile("([\\p{XDigit}\\-]+)\\s+([rwxsp\\-]+)\\s+(\\p{XDigit}+)\\s+(\\p{XDigit}+:\\p{XDigit}+)\\s+(\\d+)\\s*(.*)?");
    private static final int ADDRESS_INDEX = 1;
    private static final int PERMS_INDEX = 2;
    private static final int OFFSET_INDEX = 3;
    private static final int DEV_INDEX = 4;
    private static final int INODE_INDEX = 5;
    private static final int PATH_INDEX = 6;

    /**
     * Get the distinct files that are currently mapped to the current process
     * <p>
     * NOTE: this is only likely to work on linux or linux-like environments
     *
     * @return A set of the distinct files listed in /proc/self/maps
     */
    public static Set<String> getAllMappedFiles() {
        final Set<String> fileList = new HashSet<>();

        if (Files.exists(PROC_SELF_MAPS) && Files.isReadable(PROC_SELF_MAPS)) {
            try (final BufferedReader reader = new BufferedReader(new InputStreamReader(Files.newInputStream(PROC_SELF_MAPS)))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    final Matcher matcher = parseMapsLine(line);
                    if (matcher.matches()) {
                        String filename = getPath(matcher);
                        if (filename.startsWith("/")) {
                            fileList.add(filename);
                        } else {
                            if (!filename.trim().isEmpty()) {
                                Jvm.debug().on(MappedFileUtil.class, "Ignoring non-file " + filename);
                            }
                        }
                    } else {
                        Jvm.warn().on(MappedFileUtil.class, "Found non-matching line in /proc/self/maps: " + line);
                    }
                }
            } catch (IOException e) {
                throw new IllegalStateException("Getting mapped files failed", e);
            }
            return fileList;
        } else {
            throw new UnsupportedOperationException("This only works on systems that have a /proc/self/maps (exists="
                    + Files.exists(PROC_SELF_MAPS) + ", isReadable=" + Files.isReadable(PROC_SELF_MAPS) + ")");
        }
    }

    public static Matcher parseMapsLine(String line) {
        return LINE_PATTERN.matcher(line);
    }

    public static String getPath(Matcher matcher) {
        return matcher.group(PATH_INDEX);
    }

    public static String getAddress(Matcher matcher) {
        return matcher.group(ADDRESS_INDEX);
    }
}
