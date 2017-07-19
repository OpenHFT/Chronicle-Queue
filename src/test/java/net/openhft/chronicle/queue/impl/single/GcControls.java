package net.openhft.chronicle.queue.impl.single;

import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

final class GcControls {
    static void requestGcCycle() {
        System.gc();
    }

    static void waitForGcCycle() {
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

    private static long getGcCount() {
        return ManagementFactory.getGarbageCollectorMXBeans().stream().
                reduce(0L,
                        (count, gcBean) -> count + gcBean.getCollectionCount(),
                        (a, b) -> a + b);
    }
}
