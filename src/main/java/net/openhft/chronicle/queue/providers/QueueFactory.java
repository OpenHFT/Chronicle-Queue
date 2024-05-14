package net.openhft.chronicle.queue.providers;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

public interface QueueFactory {

    // SPI for SingleChronicleQueueBuilder
    boolean areEnterpriseFeaturesAvailable();

    SingleChronicleQueue newInstance(SingleChronicleQueueBuilder queueBuilder);
}
