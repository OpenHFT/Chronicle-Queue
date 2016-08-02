package net.openhft.chronicle.queue.impl;

import java.io.File;

/**
 * Created by peter on 27/06/16.
 */
@FunctionalInterface
public interface StoreFileListener {
    default void onAcquired(int cycle, File file) {

    }

    void onReleased(int cycle, File file);
}
