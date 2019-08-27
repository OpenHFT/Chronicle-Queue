/*
 * Copyright (c) 2016-2019 Chronicle Software Ltd
 */

package net.openhft.chronicle.queue.watcher;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.watcher.FileClassifier;
import net.openhft.chronicle.core.watcher.FileManager;
import net.openhft.chronicle.core.watcher.WatcherListener;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.table.SingleTableStore;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArraySet;

public class QueueWatcherListener implements WatcherListener {
    final Map<Path, QueueFileManager> queueFileManagerMap = new TreeMap<>();

    @Override
    public void onExists(String base, String filename, Boolean modified) throws IllegalStateException {
        if (filename.endsWith(SingleTableStore.SUFFIX) ||
                filename.endsWith(SingleChronicleQueue.SUFFIX)) {
            onExistsCQ(base, filename);
        }
    }

    void onExistsCQ(String base, String filename) {
        Path path = Paths.get(base, filename);
        Path parent = path.getParent();
        QueueFileManager fileManager = queueFileManagerMap.get(parent);
        if (fileManager == null) {
            Jvm.warn().on(getClass(), "File " + base + " " + filename + " classified as Queue");
            fileManager = new QueueFileManager(base, Paths.get(filename).getParent().toString());
            fileManager.start();
        }
        fileManager.onExists(path.getFileName().toString());
    }

    @Override
    public void onRemoved(String base, String filename) throws IllegalStateException {
        Path path = Paths.get(base, filename);
        Path parent = path.getParent();
        QueueFileManager fileManager = queueFileManagerMap.get(parent);
        fileManager.onRemoved(path.getFileName().toString());
        if (fileManager.isEmpty())
            fileManager.stop();
    }
}
