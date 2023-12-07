package net.openhft.chronicle.queue.impl.single.namedtailer;

import net.openhft.chronicle.core.values.LongValue;

import java.io.Closeable;

/**
 * Facilitates updating the persistent index of a named tailer.
 */
public interface IndexUpdater extends Closeable {

    /**
     * Update the persistent index of the named tailer.
     *
     * @param index new index value
     */
    void update(long index);

    /**
     * @return current index value
     */
    LongValue index();

}
