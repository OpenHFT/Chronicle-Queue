package net.openhft.chronicle.queue.internal;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

public final class AnalyticsMain {

    public static final String BASE_QUEUE_NAME = "analytics_test_queue";
    public static final String QUEUE_NAME1 = BASE_QUEUE_NAME + "1";
    public static final String QUEUE_NAME2 = BASE_QUEUE_NAME + "2";

    public static void main(String[] args) {

        System.out.println("Creating two queues and sending analytics...");
        System.out.println("Currently, there is a limit of four message per h per JVM instance so only some analytics message might be sent upstream.");
        try (SingleChronicleQueue q1 = SingleChronicleQueueBuilder.binary(BASE_QUEUE_NAME + "1").build()) {
            try (SingleChronicleQueue q2 = SingleChronicleQueueBuilder.binary(BASE_QUEUE_NAME + "2").rollCycle(RollCycles.HOURLY).build()) {

            }
        } finally {
            IOTools.deleteDirWithFiles(QUEUE_NAME1, QUEUE_NAME2);
        }
        Jvm.pause(2_000);
        System.out.println("Completed successfully");
        System.exit(0);
    }

}