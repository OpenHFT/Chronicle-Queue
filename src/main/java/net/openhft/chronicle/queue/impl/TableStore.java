package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.core.values.LongValue;

public interface TableStore extends CommonStore {
    LongValue acquireValueFor(CharSequence key);
}
