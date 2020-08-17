package net.openhft.chronicle.queue.impl.single;

import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public final class GcControls {
    static void requestGcCycle() {
        System.gc();
    }

    public static void waitForGcCycle() {
        final long gcCount = getGcCount();
        System.gc();
        final long timeoutAt = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(1L);
        while ((getGcCount() == gcCount) && System.currentTimeMillis() < timeoutAt) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10L));
        }

        if (getGcCount() == gcCount) {
            throw new IllegalStateException("GC did not occur within timeout");
        }
    }

    static long getGcCount() {
        return ManagementFactory.getGarbageCollectorMXBeans().stream().
                reduce(0L,
                        (count, gcBean) -> count + gcBean.getCollectionCount(),
                        Long::sum);
    }
}
