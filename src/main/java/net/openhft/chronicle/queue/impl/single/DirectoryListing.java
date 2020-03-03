package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.io.Closeable;

import java.io.File;

public interface DirectoryListing extends Closeable {
    default void init() {
    }

    default void refresh() {
    }

    void onFileCreated(File file, int cycle);

    int getMaxCreatedCycle();

    int getMinCreatedCycle();

    long modCount();
}
