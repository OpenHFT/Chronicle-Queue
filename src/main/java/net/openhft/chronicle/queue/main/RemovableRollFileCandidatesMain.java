package net.openhft.chronicle.queue.main;

import net.openhft.chronicle.queue.internal.main.InternalRemovableRollFileCandidatesMain;

public final class RemovableRollFileCandidatesMain {

    /**
     * Produces a list of removable roll file candidates and prints
     * their absolute path to standard out row-by-row.
     *
     * @param args the directory. If no directory is given, "." is assumed
     */
    public static void main(String[] args) {
        InternalRemovableRollFileCandidatesMain.main(args);
    }
}
