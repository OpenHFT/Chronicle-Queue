package net.openhft.chronicle.queue.service;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.queue.ChronicleQueue;

/**
 * Created by peter on 23/04/16.
 */
public interface ServiceWrapper extends Closeable {
    ChronicleQueue[] inputQueues();

    ChronicleQueue outputQueue();
}
