package net.openhft.chronicle.queue.util;

public final class QueueUtil {

    private QueueUtil() {}

    public static int testBlockSize() {
        return  64 * 1024; // smallest safe block size for Windows 8+
    }
}
