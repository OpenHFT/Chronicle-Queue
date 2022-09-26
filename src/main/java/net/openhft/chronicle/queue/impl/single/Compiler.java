package net.openhft.chronicle.queue.impl.single;

import java.io.File;

public class Compiler {

    public static final File FILE = new File(".");

    public static void enable() {
        FILE.exists();
    }
}
