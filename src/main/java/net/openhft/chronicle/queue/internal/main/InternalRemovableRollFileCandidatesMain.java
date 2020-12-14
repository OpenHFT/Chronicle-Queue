package net.openhft.chronicle.queue.internal.main;

import net.openhft.chronicle.queue.util.FileUtil;

import java.io.File;

public final class InternalRemovableRollFileCandidatesMain {
    /**
     * Produces a list of removable roll file candidates and prints
     * their absolute path to standard out row-by-row.
     *
     * @param args the directory. If no directory is given, "." is assumed
     */
    public static void main(String[] args) {
        final File dir;
        if (args.length == 0) {
            dir = new File(".");
        } else {
            dir = new File(args[0]);
        }
        FileUtil.removableRollFileCandidates(dir)
                .map(File::getAbsolutePath)
                .forEach(System.out::println);
    }
}
