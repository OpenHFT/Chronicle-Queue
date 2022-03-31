package net.openhft.chronicle.queue.reader;

public interface QueueEntryReader {

    /**
     * Read/process the next entry from the queue
     *
     * @return true if there was an entry to read, false if we are at the end of the queue
     */
    boolean read();
}
