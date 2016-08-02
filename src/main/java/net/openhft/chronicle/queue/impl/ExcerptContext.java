package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.wire.Wire;

/**
 * Created by peter on 07/07/16.
 */
public interface ExcerptContext {
    Wire wire();

    Wire wireForIndex();

    long timeoutMS();

    default void pauserReset() {
        wire().pauser().reset();
        wireForIndex().pauser().reset();
    }
}
