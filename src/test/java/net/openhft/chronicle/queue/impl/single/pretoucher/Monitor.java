package net.openhft.chronicle.queue.impl.single.pretoucher;

import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.threads.MonitorEventLoop;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.threads.PauserMonitor;
import net.openhft.chronicle.threads.ThreadMonitors;
import org.jetbrains.annotations.NotNull;

import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;

@Deprecated(/* To be removed in x.23 */)
public class Monitor {
    static final EventLoop loop = new MonitorEventLoop(null, Pauser.millis(10));

    static {
        loop.start();
    }

    public static void addTimingMonitor(String description, long timeLimit, LongSupplier timeSupplier, Supplier<Thread> threadSupplier) {
        loop.addHandler(ThreadMonitors.forThread(description, timeLimit, timeSupplier, threadSupplier));
    }

    public static void addPauser(String desc, Pauser pauser) {
        loop.addHandler(new PauserMonitor(pauser, desc, 3));
    }

    public static void addHeartbeatSource(long heartbeatMS,
                                          @NotNull Supplier<LongConsumer> heartbeatReceiverMethodSupplier) {
        long heartbeatUS = heartbeatMS * 1000;
        loop.addHandler(new PeriodicUpdateEventHandler(heartbeatReceiverMethodSupplier, 0, heartbeatUS));
    }

    public static void addPeriodicUpdateSource(long periodicUpdateMS,
                                               @NotNull Supplier<LongConsumer> updateReceiverMethodSupplier) {
        long periodicUpdateUS = periodicUpdateMS * 1000;
        loop.addHandler(new PeriodicUpdateEventHandler(updateReceiverMethodSupplier, 0, periodicUpdateUS));
    }

    public static void addPeriodicUpdateSource(long startTimeMillis, long periodicUpdateMS,
                                               @NotNull Supplier<LongConsumer> updateReceiverMethodSupplier) {
        long periodicUpdateUS = periodicUpdateMS * 1000;
        loop.addHandler(new PeriodicUpdateEventHandler(updateReceiverMethodSupplier, startTimeMillis, periodicUpdateUS));
    }

    public static void close() {
        closeQuietly(loop);
    }
}
