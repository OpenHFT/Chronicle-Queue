/*
 * Copyright 2014-2020 chronicle.software
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
package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.impl.single.TableStoreWriteLock;
import net.openhft.chronicle.queue.impl.table.Metadata;
import net.openhft.chronicle.queue.impl.table.SingleTableBuilder;
import net.openhft.chronicle.threads.BusyTimedPauser;
import org.jetbrains.annotations.NotNull;

import java.io.File;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueue.QUEUE_METADATA_FILE;

@Deprecated /* For removal in x.22, use net.openhft.chronicle.queue.main.UnlockMain instead */
public class QueueUnlockMain {
    static {
        SingleChronicleQueueBuilder.addAliases();
    }

    public static void main(String[] args) {
        unlock(args[0]);
    }

    private static void unlock(@NotNull String dir) {
        File path = new File(dir);
        if (!path.isDirectory()) {
            System.err.println("Path argument must be a queue directory");
            System.exit(1);
        }

        File storeFilePath = new File(path, QUEUE_METADATA_FILE);

        if (!storeFilePath.exists()) {
            System.err.println("Metadata file not found, nothing to unlock");
            System.exit(1);
        }

        final TableStore<?> store = SingleTableBuilder.binary(storeFilePath, Metadata.NoMeta.INSTANCE).readOnly(false).build();

        // appender lock
        (new TableStoreWriteLock(store, BusyTimedPauser::new, 0L, TableStoreWriteLock.APPEND_LOCK_KEY)).forceUnlock();

        // write lock
        (new TableStoreWriteLock(store, BusyTimedPauser::new, 0L)).forceUnlock();

        System.out.println("Done");
    }
}