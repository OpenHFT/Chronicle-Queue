package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.providers.QueueFactory;

public class SingleChronicleQueueFactory implements QueueFactory {

    @Override
    public boolean areEnterpriseFeaturesAvailable() {
        return false;
    }

    @Override
    public SingleChronicleQueue newInstance(SingleChronicleQueueBuilder queueBuilder) {
        return new SingleChronicleQueue(queueBuilder);
    }
}
