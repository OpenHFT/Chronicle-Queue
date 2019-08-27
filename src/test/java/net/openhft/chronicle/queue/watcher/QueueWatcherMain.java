/*
 * Copyright (c) 2016-2019 Chronicle Software Ltd
 */

package net.openhft.chronicle.queue.watcher;

import net.openhft.chronicle.core.watcher.FileSystemWatcher;

import java.io.File;
import java.io.IOException;

public class QueueWatcherMain {
    public static void main(String[] args) throws IOException, InterruptedException {
        FileSystemWatcher fsw = new FileSystemWatcher();
        String absolutePath = new File(".").getAbsolutePath();
        System.out.println("Watching " + absolutePath);
        fsw.addPath(absolutePath);
        QueueWatcherListener listener = new QueueWatcherListener();
        fsw.addListener(listener);
        fsw.start();
        Thread.currentThread().join();
    }
}
