package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.queue.impl.single.TableStoreWriteLock;
import net.openhft.chronicle.queue.impl.table.Metadata;
import net.openhft.chronicle.queue.impl.table.SingleTableBuilder;
import net.openhft.chronicle.threads.Pauser;

import java.nio.file.Paths;

/**
 * Acquire a lock and just hold it until we're interrupted
 */
public class TableStoreWriteLockLockerProcess {

    public static void main(String[] args) {
        try (TableStore<Metadata.NoMeta> tableStore = SingleTableBuilder.binary(Paths.get(args[0]), Metadata.NoMeta.INSTANCE).build();
             TableStoreWriteLock writeLock = new TableStoreWriteLock(tableStore, Pauser::balanced, 1_000L, args[1])) {
            writeLock.lock();
            while (!Thread.currentThread().isInterrupted()) {
                Jvm.pause(10);
            }
            writeLock.unlock();
        }
    }
}
