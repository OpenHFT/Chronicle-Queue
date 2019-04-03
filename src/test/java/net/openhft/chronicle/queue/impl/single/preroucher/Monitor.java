package net.openhft.chronicle.queue.impl.single.preroucher;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.threads.MonitorEventLoop;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.threads.PauserMonitor;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;

public class Monitor {
    static final EventLoop loop = new MonitorEventLoop(null, Pauser.millis(10));
    private static final Logger LOG = LoggerFactory.getLogger(Monitor.class);

    static {
        loop.start();
    }

    public static void addTimingMonitor(String description, long timeLimit, LongSupplier timeSupplier, Supplier<Thread> threadSupplier) {
        loop.addHandler(new ThreadMonitorEventHandler(description, timeLimit, timeSupplier, threadSupplier));
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

    static class ThreadMonitorEventHandler implements EventHandler {
        private final String description;
        private final long timeLimit;
        private final LongSupplier timeSupplier;
        private final Supplier<Thread> threadSupplier;
        private long lastTime = 0;

        ThreadMonitorEventHandler(String description, long timeLimit, LongSupplier timeSupplier, Supplier<Thread> threadSupplier) {
            this.description = description;
            this.timeLimit = timeLimit;
            this.timeSupplier = timeSupplier;
            this.threadSupplier = threadSupplier;
        }

        @Override
        public boolean action() {
            long time = timeSupplier.getAsLong();
            if (time == Long.MIN_VALUE)
                return false;
            long latency = System.nanoTime() - time;
            if (latency > timeLimit) {
                Thread thread = threadSupplier.get();
                if (thread != null && thread.isAlive() && LOG.isInfoEnabled()) {
                    String type = (time == lastTime) ? "re-reporting" : "new report";
                    StringBuilder out = new StringBuilder().append("THIS IS NOT AN ERROR, but a profile of the thread, ").append(description).append(" thread ").append(thread.getName()).append(" blocked for ").append(latency / 1000000).append(" ms. ").append(type);
                    Jvm.trimStackTrace(out, thread.getStackTrace());

                    LOG.info(out.toString());
                    lastTime = time;
                }
            }
            return false;
        }
    }
}
