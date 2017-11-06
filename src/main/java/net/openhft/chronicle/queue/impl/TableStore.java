package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.core.values.LongValue;

import java.util.function.Function;

public interface TableStore extends CommonStore {

    /**
     * Acquire {@link LongValue} mapped to underlying file, providing atomic operations on the value that is shared
     * across threads and/or JVMs.
     * Note: The implementation of this method is not required to guarantee that if the value does not exist in the file,
     * it will create one and only one value in the file in case of concurrent access. On the contrary, it's possible
     * that different threads or processes acquire {@link LongValue}s pointing to different fields in the underlying
     * file. To prevent this, it is advised to use {@link #doWithExclusiveLock(Function)} to wrap calls to this method,
     * which will ensure exclusive access to file while initially acquiring values.
     *
     * @param key the key of the value
     * @return {@link LongValue} object pointing to particular location in mapped underlying file
     */
    LongValue acquireValueFor(CharSequence key);

    /**
     * Acquires file-system level lock on the underlying file, to prevent concurrent access from multiple processes.
     * It is recommended to use this when acquiring your values for the first time, otherwise it is possible to get
     * unpredictable results in case of multiple processes/threads trying to acquire values for the same key.
     * In addition, it allows to batch multiple {@link #acquireValueFor(CharSequence)} calls, to atomically acquire
     * multiple values.
     *
     * @param code code block to execute using locked table store
     * @param <R>  result type
     * @return result of code block execution
     */
    <R> R doWithExclusiveLock(Function<TableStore, ? extends R> code);
}
