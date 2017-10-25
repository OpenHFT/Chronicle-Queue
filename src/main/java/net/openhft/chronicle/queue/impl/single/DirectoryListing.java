package net.openhft.chronicle.queue.impl.single;

import java.io.File;

public interface DirectoryListing {
    void init();

    void refresh();

    void onFileCreated(File file, int cycle);

    int getMaxCreatedCycle();

    int getMinCreatedCycle();
}
