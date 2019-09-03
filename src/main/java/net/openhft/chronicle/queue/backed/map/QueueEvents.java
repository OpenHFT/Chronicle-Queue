/*
 * Copyright (c) 2016-2019 Chronicle Software Ltd
 */

package net.openhft.chronicle.queue.backed.map;

import net.openhft.chronicle.wire.MethodWriterWithContext;

// TODO add queue names and only read the ones for it.
public interface QueueEvents<K, V> extends MethodWriterWithContext {
    /**
     * @param key       to put
     * @param value     to put
     * @param timestamp last batch timestamp
     */
    void $put(String name, K key, V value, long timestamp);

    /**
     * @param key       to remove
     * @param timestamp last batch timestamp
     */
    void $remove(String name, K key, long timestamp);

    /**
     * Remove all entries
     *
     * @param timestamp last batch timestamp
     */
    void $clear(String name, long timestamp);

    /**
     * @param hostId which caused the checkpoint
     */
    void $checkPoint(String name, int hostId);
}
