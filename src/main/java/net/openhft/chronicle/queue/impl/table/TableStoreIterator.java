package net.openhft.chronicle.queue.impl.table;

import net.openhft.chronicle.wire.ValueIn;

public interface TableStoreIterator<A> {
    void accept(A accumulator, CharSequence key, ValueIn value);
}
