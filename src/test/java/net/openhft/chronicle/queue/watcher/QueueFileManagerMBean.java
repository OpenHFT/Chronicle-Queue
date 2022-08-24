/*
 * Copyright (c) 2016-2020 chronicle.software
 */

package net.openhft.chronicle.queue.watcher;

import java.util.Set;

@SuppressWarnings("unused")
@Deprecated(/* for removal in x.24 */)
public interface QueueFileManagerMBean {
    Set<String> getFiles();

    String getTableStore();

    String getLastHeader();
}
