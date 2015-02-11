package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.MultiStoreBytes;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by peter.lawrey on 03/02/15.
 */
public interface DirectChronicleQueue extends ChronicleQueue {
    void appendDocument(Bytes buffer);

    boolean readDocument(AtomicLong offset, Bytes buffer);

    Bytes bytes();

    long lastIndex();

    boolean index(long index, MultiStoreBytes bytes);

    long firstBytes();
}
