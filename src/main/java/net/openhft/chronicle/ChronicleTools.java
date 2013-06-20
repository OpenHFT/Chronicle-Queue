package net.openhft.chronicle;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

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

    enum DeleteStatic {
        INSTANCE;

        final List<String> toDeleteList = new ArrayList<String>();

        {
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    for (String dir : toDeleteList) {
                        System.out.println("Deleting " + dir);
                        deleteDir(dir);
                    }
                }
            }));
        }

        synchronized void add(String dirPath) {
            deleteDir(dirPath);
            toDeleteList.add(dirPath);
        }

        private void deleteDir(String dirPath) {
            File dir = new File(dirPath);
            // delete one level.
            if (dir.isDirectory()) {
                for (File file : dir.listFiles())
                    file.delete();
            }
            dir.delete();
        }
    }

    public static void deleteDirOnExit(String dirPath) {
        DeleteStatic.INSTANCE.add(dirPath);
    }
}
