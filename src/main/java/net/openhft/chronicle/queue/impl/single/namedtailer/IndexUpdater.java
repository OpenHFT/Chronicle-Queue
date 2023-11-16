package net.openhft.chronicle.queue.impl.single.namedtailer;

import net.openhft.chronicle.core.values.LongValue;

import java.io.Closeable;

public interface IndexUpdater extends Closeable {

    void update(long index);

    LongValue index();

}
