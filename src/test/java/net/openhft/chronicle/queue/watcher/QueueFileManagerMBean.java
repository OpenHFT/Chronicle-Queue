/*
 * Copyright (c) 2016-2019 Chronicle Software Ltd
 */

package net.openhft.chronicle.queue.watcher;

import java.util.Set;

public interface QueueFileManagerMBean {
    public Set<String> getFiles();
}
