package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.core.values.LongValue;

import java.util.function.Function;

public interface TableStore extends CommonStore {
    <R> R doWithExclusiveLock(Function<TableStore, ? extends R> code);
    LongValue acquireValueFor(CharSequence key);
}
