package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.io.Closeable;

import java.io.File;

public interface DirectoryListing extends Closeable {
    default void init() {
    }

    void refresh(boolean force);

    void onFileCreated(File file, int cycle);

    int getMinCreatedCycle();

    int getMaxCreatedCycle();

    long modCount();
}
