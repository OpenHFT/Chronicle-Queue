package net.openhft.chronicle.queue.bench.multiprocess;

import net.openhft.posix.ClockId;
import net.openhft.posix.PosixAPI;

public class ClockUtil {
    public static long getNanoTime() {
        return PosixAPI.posix().clock_gettime(ClockId.CLOCK_REALTIME);

    }
}
