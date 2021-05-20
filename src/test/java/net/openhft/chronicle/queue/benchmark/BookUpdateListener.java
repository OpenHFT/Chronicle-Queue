package net.openhft.chronicle.queue.benchmark;

import net.openhft.chronicle.bytes.MethodId;


public interface BookUpdateListener {
    @MethodId(1)
    void bookUpdate(BookUpdate bookUpdate);
}
