package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.queue.impl.table.SingleTableBuilder;

import java.io.File;

public interface DirectoryListing extends Closeable {
    String DIRECTORY_LISTING_FILE = "directory-listing" + SingleTableBuilder.SUFFIX;

    void init();

    void refresh();

    void onFileCreated(File file, int cycle);

    int getMaxCreatedCycle();

    int getMinCreatedCycle();

    long modCount();
}
