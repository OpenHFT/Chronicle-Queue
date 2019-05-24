/*
 * Copyright 2014-2018 Chronicle Software
 *
 * http://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.threads.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ExecutorService;

public enum QueueFileShrinkManager {
    ;
    public static final String THREAD_NAME = "queue-file-shrink-daemon";
    static boolean RUN_SYNCHRONOUSLY = false;
    private static final Logger LOG = LoggerFactory.getLogger(QueueFileShrinkManager.class);
    private static final boolean DISABLE_QUEUE_FILE_SHRINKING = OS.isWindows() || Boolean.getBoolean("chronicle.queue.disableFileShrinking");
    private static final ExecutorService executor = Threads.acquireExecutorService(THREAD_NAME, 1, true);

    public static void scheduleShrinking(File queueFile, long writePos) {
        if (DISABLE_QUEUE_FILE_SHRINKING)
            return;
        Runnable task = () -> {
            while (true) {
                try {
                    if (LOG.isDebugEnabled())
                        LOG.debug("Shrinking {} to {}", queueFile, writePos);
                    RandomAccessFile raf = new RandomAccessFile(queueFile, "rw");

                    raf.setLength(writePos);
                    raf.close();
                } catch (IOException ex) {
                    // on macrosux windows, keep retrying until the file is unmapped
                    if (ex.getMessage().contains("The requested operation cannot be performed on a file with a user-mapped section open"))
                        continue;
                    LOG.warn("Failed to shrink file " + queueFile, ex);
                }
                break;
            }
        };
        if (RUN_SYNCHRONOUSLY)
            task.run();
        else
            executor.submit(task);
    }
}
