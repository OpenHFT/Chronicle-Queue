package net.openhft.chronicle.queue;

/**
 * Created by Peter on 05/03/2016.
 */
public enum TailerDirection {
    NONE, // don't move after a read.
    FORWARD, // move to the next entry
    BACKWARD // move to the previous entry.
}
