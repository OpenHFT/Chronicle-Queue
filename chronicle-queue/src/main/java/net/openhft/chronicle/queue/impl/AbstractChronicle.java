package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.wire.Wire;

/**
 * Created by Rob Austin
 */
abstract class AbstractChronicle implements ChronicleQueue, DirectChronicleQueue {

    abstract Wire wire();

    abstract Class<? extends Wire> wireType();

    abstract long indexToIndex();

    abstract long newIndex();
}
