package net.openhft.chronicle.queue;

import net.openhft.chronicle.wire.Wire;

public interface AppenderListener {
    /**
     * Message appended with the index appended
     *
     * @param wire
     * @param index
     */
    void onExcerpt(Wire wire, long index);
}
