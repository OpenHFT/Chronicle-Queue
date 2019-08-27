/*
 * Copyright (c) 2016-2019 Chronicle Software Ltd
 */

package net.openhft.chronicle.queue.watcher;

import java.util.Set;

@SuppressWarnings("unused")
public interface QueueFileManagerMBean {
    public Set<String> getFiles();

    public String getTableStore();

    public String getLastHeader();
}
