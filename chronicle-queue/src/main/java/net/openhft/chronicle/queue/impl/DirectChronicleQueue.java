package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStoreBytes;
import net.openhft.chronicle.queue.ChronicleQueue;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by peter.lawrey on 03/02/15.
 */
public interface DirectChronicleQueue extends ChronicleQueue {

    /**
     * @param buffer the bytes of the document
     * @return the index of the document appended
     */
    long appendDocument(Bytes buffer);

    boolean readDocument(AtomicLong offset, Bytes buffer);

    Bytes bytes();

    /**
     * @return the last index in the chronicle
     * @throws java.lang.IllegalStateException if now data has been written tot he chronicle
     */
    long lastIndex();

    boolean index(long index, BytesStoreBytes<?> bytes);

    long firstBytes();
}
