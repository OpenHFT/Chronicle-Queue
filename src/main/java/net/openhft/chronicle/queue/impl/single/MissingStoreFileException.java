package net.openhft.chronicle.queue.impl.single;

/**
 * Thrown when a store file we expect to be present is missing (probably because it was deleted)
 */
public class MissingStoreFileException extends IllegalStateException {
    public MissingStoreFileException(String s) {
        super(s);
    }
}
