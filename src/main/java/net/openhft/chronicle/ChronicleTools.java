package net.openhft.chronicle;

import java.io.File;

/**
 * @author peter.lawrey
 */
public enum ChronicleTools {
    ;

    /**
     * Delete a chronicle now and on exit, for testing
     *
     * @param basePath of the chronicle
     */
    public static void deleteOnExit(String basePath) {
        for (String name : new String[]{basePath + ".data", basePath + ".index"}) {
            File file = new File(name);
            //noinspection ResultOfMethodCallIgnored
            file.delete();
            file.deleteOnExit();
        }
    }
}
