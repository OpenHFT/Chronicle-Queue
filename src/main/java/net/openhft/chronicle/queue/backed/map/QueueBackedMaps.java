/*
 * Copyright (c) 2016-2019 Chronicle Software Ltd
 */

package net.openhft.chronicle.queue.backed.map;

import net.openhft.chronicle.wire.DocumentContext;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Used by services to handle multiple QueueBackedMaps
 */
public class QueueBackedMaps implements QueueEvents {
    final Map<String, QueueEvents> maps = new ConcurrentHashMap<>();

    public void addMap(String name, QueueEvents listener) {
        maps.put(name, listener);
    }

    @Override
    public DocumentContext writingDocument() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void $put(String name, Object key, Object value, long timestamp) {
        maps.computeIfPresent(name, (k, map) -> {
            map.$put(name, key, value, timestamp);
            return map;
        });
    }

    @Override
    public void $remove(String name, Object key, long timestamp) {
        maps.computeIfPresent(name, (k, map) -> {
            map.$remove(name, key, timestamp);
            return map;
        });
    }

    @Override
    public void $clear(String name, long timestamp) {
        maps.computeIfPresent(name, (k, map) -> {
            map.$clear(name, timestamp);
            return map;
        });
    }

    @Override
    public void $checkPoint(String name, int hostId) {
        maps.computeIfPresent(name, (k, map) -> {
            map.$checkPoint(name, hostId);
            return map;
        });
    }
}
