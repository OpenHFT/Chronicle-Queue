/*
 * Copyright (c) 2016-2019 Chronicle Software Ltd
 */

package net.openhft.chronicle.queue.watcher;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.watcher.FileSystemWatcher;

import java.io.File;
import io.hawt.embedded.Main;

public class QueueWatcherMain {
    public static void main(String[] args) throws Exception {
        FileSystemWatcher fsw = new FileSystemWatcher();
        String absolutePath = new File(".").getAbsolutePath();
        System.out.println("Watching " + absolutePath);
        fsw.addPath(absolutePath);
        QueueWatcherListener listener = new QueueWatcherListener();
        fsw.addListener(listener);
        fsw.start();

        System.setProperty("hawtio.authenticationEnabled", "false");
        Main main = new Main();
        main.setWar(Jvm.userHome() +"/OpenHFT/hawtio-default-2.7.1.war");
        main.run();
    }
}
