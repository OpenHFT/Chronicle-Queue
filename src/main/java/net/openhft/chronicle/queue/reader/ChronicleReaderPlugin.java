package net.openhft.chronicle.queue.reader;

import net.openhft.chronicle.wire.DocumentContext;

import java.util.function.Consumer;

/**
 * Handle the document from the queue that is read in <code>ChronicleReader</code>.
 * Particularly useful when you need more than the text representation e.g.
 * when your queue is written in binary.
 */
public interface ChronicleReaderPlugin {
    void onReadDocument(DocumentContext dc);

    /**
     * Consume dc and allow it to be given back to ChronicleReader so it could e.g. apply inclusion filters
     *
     * @param dc              doc context
     * @param messageConsumer use this to pass back text representation
     */
    default void onReadDocument(DocumentContext dc, Consumer<String> messageConsumer) {
        onReadDocument(dc);
    }
}
