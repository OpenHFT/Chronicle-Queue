/*
 * Copyright (c) 2016-2019 Chronicle Software Ltd
 */

package net.openhft.chronicle.queue.watcher;

import net.openhft.chronicle.core.watcher.JMXFileManager;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

public class QueueFileManager extends JMXFileManager implements QueueFileManagerMBean {
    private final Set<String> files = new ConcurrentSkipListSet<>();
    private static final long TIME_OUT = 5_000;
    private String tableStore;
    private long lastUpdate = 0;

    public QueueFileManager(String basePath, String relativePath) {
        super(basePath, relativePath);
    }

    @Override
    protected String type() {
        return "queues";
    }

    private String lastHeader;

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

    public void onExists(String filename) {
        files.add(filename);
    }

    public String getTableStore() {
        update();
        return tableStore;
    }

    public String getLastHeader() {
        update();
        return lastHeader;
    }

    private void update() {
        long now = System.currentTimeMillis();
        if (lastUpdate + TIME_OUT > now)
            return;

        lastUpdate = now;
        Path path = Paths.get(basePath, relativePath);
        try (SingleChronicleQueue queue =
                     SingleChronicleQueueBuilder.single(path.toFile())
                             .readOnly(true)
                             .build()) {
            TableStore ts = queue.metaStore();
            tableStore = ts.shortDump();
            lastHeader = queue.dumpLastHeader();
        }
    }
}
