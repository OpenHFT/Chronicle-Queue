package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.queue.Chronicle;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.MultiStoreBytes;

/**
 * Created by peter.lawrey on 03/02/15.
 */
public interface DirectChronicle extends Chronicle {
    void appendDocument(Bytes buffer);

    Bytes bytes();

    long lastIndex();

    boolean index(long index, MultiStoreBytes bytes);
}
