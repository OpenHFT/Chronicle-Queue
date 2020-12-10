package net.openhft.chronicle.queue.main;

import net.openhft.chronicle.queue.internal.main.InternalPingPongMain;

/**
 * System Properties:
 *
 *    static int runtime = Integer.getInteger("runtime", 30); // seconds
 *    static String basePath = System.getProperty("path", OS.TMP);
 */
public final class PingPongMain {

    public static void main(String[] args) {
        InternalPingPongMain.main(args);
    }

}