/*
 * Copyright (c) 2016-2019 Chronicle Software Ltd
 */

package net.openhft.chronicle.queue.watcher;

import net.openhft.chronicle.core.watcher.JMXFileManager;
import net.openhft.chronicle.queue.TailerDirection;

import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;

public class QueueFileManager extends JMXFileManager implements QueueFileManagerMBean{
    private final Set<String> files = new ConcurrentSkipListSet<>();

    public QueueFileManager(String basePath, String relativePath) {
        super(basePath, relativePath);
    }

    @Override
    protected String type() {
        return "queues";
    }

    public void onExists(String filename) {
        files.add(filename);
    }

    public void onRemoved(String filename) {
        files.remove(filename);
    }

    public boolean isEmpty() {
        return files.isEmpty();
    }

    @Override
    public Set<String> getFiles() {
        return files;
    }
}
