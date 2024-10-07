package net.openhft.chronicle.queue;

import org.junit.Test;
import static org.junit.Assert.*;
import java.io.File;

/**
 * Unit tests for ExcerptCommon interface implementations.
 */
public class ExcerptCommonTest {

    class ExcerptCommonImpl implements ExcerptCommon<ExcerptCommonImpl> {
        private final int sourceId;
        private final ChronicleQueue queue;
        private final File currentFile;

        public ExcerptCommonImpl(int sourceId, ChronicleQueue queue, File currentFile) {
            this.sourceId = sourceId;
            this.queue = queue;
            this.currentFile = currentFile;
        }

        @Override
        public int sourceId() {
            return sourceId;
        }

        @Override
        public ChronicleQueue queue() {
            return queue;
        }

        @Override
        public File currentFile() {
            return currentFile;
        }

        @Override
        public void sync() {
            // Sync implementation
        }

        @Override
        public void close() {
            // Close resources if necessary
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void singleThreadedCheckReset() {

        }

        @Override
        public void singleThreadedCheckDisabled(boolean singleThreadedCheckDisabled) {

        }
    }

    @Test
    public void testSourceId() {
        ChronicleQueue queue = ChronicleQueue.single("testQueue");
        ExcerptCommonImpl excerpt = new ExcerptCommonImpl(123, queue, null);
        assertEquals(123, excerpt.sourceId());
    }

    @Test
    public void testQueue() {
        ChronicleQueue queue = ChronicleQueue.single("testQueue");
        ExcerptCommonImpl excerpt = new ExcerptCommonImpl(123, queue, null);
        assertEquals(queue, excerpt.queue());
    }

    @Test
    public void testCurrentFile() {
        File file = new File("testfile.txt");
        ChronicleQueue queue = ChronicleQueue.single("testQueue");
        ExcerptCommonImpl excerpt = new ExcerptCommonImpl(123, queue, file);
        assertEquals(file, excerpt.currentFile());

        ExcerptCommonImpl excerptWithNullFile = new ExcerptCommonImpl(123, queue, null);
        assertNull(excerptWithNullFile.currentFile());
    }

    @Test
    public void testSync() {
        ChronicleQueue queue = ChronicleQueue.single("testQueue");
        ExcerptCommonImpl excerpt = new ExcerptCommonImpl(123, queue, null);
        excerpt.sync(); // Would test actual sync if implemented
        // No assertion needed for this default method
    }
}
