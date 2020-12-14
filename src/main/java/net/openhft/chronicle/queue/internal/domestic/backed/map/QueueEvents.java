/*
 * Copyright (c) 2016-2020 chronicle.software
 */

package net.openhft.chronicle.queue.internal.domestic.backed.map;

// TODO add queue names and only read the ones for it.
public interface QueueEvents<K, V> {
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
