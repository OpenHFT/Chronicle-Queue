package net.openhft.chronicle.queue.reader;

import net.openhft.chronicle.wire.DocumentContext;

/**
 * Handle the document from the queue that is read in <code>ChronicleReader</code>.
 * Particularly useful when you need more than the text representation e.g.
 * when your queue is written in binary.
 */
public interface ChronicleReaderPlugin {
    void onReadDocument(DocumentContext dc);
}
