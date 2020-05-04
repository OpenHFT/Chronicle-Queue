/*
 * Copyright (c) 2016-2019 Chronicle Software Ltd
 */

package net.openhft.chronicle.queue.watcher;

import java.util.Set;

@SuppressWarnings("unused")
public interface QueueFileManagerMBean {
    Set<String> getFiles();

    String getTableStore();

    String getLastHeader();
}
