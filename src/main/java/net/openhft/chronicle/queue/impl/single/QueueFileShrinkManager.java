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

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.threads.Threads;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ExecutorService;

public enum QueueFileShrinkManager {
    ;
    private static final Boolean DISABLE_QUEUE_FILE_SHRINKING = Boolean.getBoolean("chronicle.queue.disableFileShrinking");
    private static final ExecutorService executor = Threads.acquireExecutorService("queue-file-shrink-daemon", 1, true);

    public static void scheduleShrinking(File queueFile, long writePos) {
        if (DISABLE_QUEUE_FILE_SHRINKING)
            return;
        executor.submit(() -> {
            try {
                Jvm.warn().on(QueueFileShrinkManager.class, "Shrinking " + queueFile + " to " + writePos);
                RandomAccessFile raf = new RandomAccessFile(queueFile, "rw");

                raf.setLength(writePos);
                raf.close();
            } catch (IOException ex) {
                Jvm.warn().on(QueueFileShrinkManager.class, "Failed to shrink file " + queueFile, ex);
            }
        });
    }
}
