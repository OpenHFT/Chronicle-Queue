package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.impl.DirectChronicleQueue;
import net.openhft.chronicle.queue.impl.Indexer;
import net.openhft.chronicle.wire.WireKey;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;

/**
 * @author Rob Austin.
 */
public class SimpleChronicleQueueTest {

    enum Field implements WireKey {
        TEXT
    }

    @Test
    public void testSimpleWire() throws Exception {

        File file = File.createTempFile("chronicle.", "q");
        file.deleteOnExit();

        try {

            final DirectChronicleQueue
                    chronicle = (DirectChronicleQueue) new ChronicleQueueBuilder(file.getAbsolutePath()).build();

            final ExcerptAppender appender = chronicle.createAppender();
            appender.writeDocument(wire -> wire.write(() -> "FirstName").text("Steve"));
            appender.writeDocument(wire -> wire.write(() -> "Surname").text("Jobs"));

            StringBuilder first = new StringBuilder();
            StringBuilder surname = new StringBuilder();

            final ExcerptTailer tailer = chronicle.createTailer();

            tailer.readDocument(wire -> wire.read(() -> "FirstName").text(first));
            tailer.readDocument(wire -> wire.read(() -> "Surname").text(surname));

            Assert.assertEquals("Steve Jobs", first + " " + surname);

            Bytes bytes = chronicle.bytes();
            bytes.flip();

        } finally {
            file.delete();
        }

    }

    @Test
    public void testSimpleDirect() throws Exception {

        File file = File.createTempFile("chronicle.", "q");
        file.deleteOnExit();


        DirectChronicleQueue chronicle = (DirectChronicleQueue) new ChronicleQueueBuilder(file.getAbsolutePath()).build();

        final ExcerptAppender appender = chronicle.createAppender();

        // create 100 documents
        for (int i = 0; i < 100; i++) {
            final int j = i;
            appender.writeDocument(wire -> wire.write(() -> "key").text("value=" + j));
        }

        final ExcerptTailer tailer = chronicle.createTailer();

        for (int i = 0; i < chronicle.lastIndex(); i++) {

            tailer.readDocument(wireIn -> {
                Bytes bytes1 = wireIn.bytes();
                long remaining = bytes1.remaining();
                bytes1.skip(remaining);
            });
        }


    }

    @Test
    public void testReadAtIndex() throws Exception {

        final File file = File.createTempFile("chronicle.", "q");
        file.deleteOnExit();

        try {

            DirectChronicleQueue chronicle = (DirectChronicleQueue) new ChronicleQueueBuilder(file.getAbsolutePath()).build();

            final ExcerptAppender appender = chronicle.createAppender();

            // create 100 documents
            for (int i = 0; i < 100; i++) {
                final int j = i;
                appender.writeDocument(wire -> wire.write(() -> "key").text("value=" + j));
            }

            final ExcerptTailer tailer = chronicle.createTailer();
            tailer.index(5);

            //   QueueDumpMain.dump(file, new PrintWriter(System.out));
/*
            StringBuilder sb = new StringBuilder();
            tailer.readDocument(wire -> wire.read(() -> "key").text(sb));

            Assert.assertEquals("value=5", sb.toString());
*/

        } finally {
            file.delete();
        }

    }

    @Test
    public void testLastWrittenIndexPerAppender() throws Exception {

        File file = File.createTempFile("chronicle.", "q");
        file.deleteOnExit();
        try {

            DirectChronicleQueue chronicle = (DirectChronicleQueue) new ChronicleQueueBuilder(file.getAbsolutePath()).build();

            final ExcerptAppender appender = chronicle.createAppender();

            appender.writeDocument(wire -> wire.write(() -> "key").text("test"));
            Assert.assertEquals(0, appender.lastWrittenIndex());

        } finally {
            file.delete();
        }

    }


    @Test(expected = IllegalStateException.class)
    public void testLastWrittenIndexPerAppenderNoData() throws Exception {

        File file = File.createTempFile("chronicle.", "q");
        file.deleteOnExit();
        try {

            DirectChronicleQueue chronicle = (DirectChronicleQueue) new ChronicleQueueBuilder(file.getAbsolutePath()).build();

            final ExcerptAppender appender = chronicle.createAppender();
            appender.lastWrittenIndex();

        } finally {
            file.delete();
        }

    }

    @Test
    public void testLastIndexPerChronicle() throws Exception {

        File file = File.createTempFile("chronicle.", "q");
        file.deleteOnExit();
        try {

            DirectChronicleQueue chronicle = (DirectChronicleQueue) new ChronicleQueueBuilder(file.getAbsolutePath()).build();
            final ExcerptAppender appender = chronicle.createAppender();

            appender.writeDocument(wire -> wire.write(() -> "key").text("test"));
            Assert.assertEquals(0, chronicle.lastIndex());

        } finally {
            file.delete();
        }

    }


    @Test(expected = IllegalStateException.class)
    public void testLastIndexPerChronicleNoData() throws Exception {

        File file = File.createTempFile("chronicle.", "q");
        file.deleteOnExit();
        try {

            DirectChronicleQueue chronicle = (DirectChronicleQueue) new ChronicleQueueBuilder(file.getAbsolutePath()).build();
            Assert.assertEquals(0, chronicle.lastIndex());

        } finally {
            file.delete();
        }

    }


    @Test
    @Ignore
    public void testHeaderIndexReadAtIndex() throws Exception {

        File file = File.createTempFile("chronicle.", "q");
        file.deleteOnExit();
        try {

            DirectChronicleQueue chronicle = (DirectChronicleQueue) new ChronicleQueueBuilder(file.getAbsolutePath()).build();

            final ExcerptAppender appender = chronicle.createAppender();

            // create 100 documents
            for (int i = 0; i < 100; i++) {
                final int j = i;
                appender.writeDocument(wire -> wire.write(() -> "key").text("value=" + j));
            }

            final ExcerptTailer tailer = chronicle.createTailer();
            tailer.index(5);

            StringBuilder sb = new StringBuilder();
            tailer.readDocument(wire -> wire.read(() -> "key").text(sb));

            Assert.assertEquals("value=5", sb.toString());


        } finally {
            file.delete();
        }

    }


    @Test
    @Ignore
    public void testReadAtIndexWithIndexes() throws Exception {

        File file = File.createTempFile("chronicle.", "q");
        file.deleteOnExit();
        try {

            DirectChronicleQueue chronicle = (DirectChronicleQueue) new ChronicleQueueBuilder(file.getAbsolutePath()).build();

            final ExcerptAppender appender = chronicle.createAppender();

            // create 100 documents
            for (int i = 0; i < 100; i++) {
                final int j = i;
                appender.writeDocument(wire -> wire.write(() -> "key").text("value=" + j));
            }

            final ExcerptTailer tailer = chronicle.createTailer();

            // creates the indexes
            Indexer.index(chronicle);

            tailer.index(67);

            StringBuilder sb = new StringBuilder();
            tailer.readDocument(wire -> wire.read(() -> "key").text(sb));

            Assert.assertEquals("value=67", sb.toString());

        } finally {
            file.delete();
        }

    }


    @Test
    @Ignore
    public void testReadAtIndexWithIndexesAtStart() throws Exception {

        File file = File.createTempFile("chronicle.", "q");
        file.deleteOnExit();
        try {

            DirectChronicleQueue chronicle = (DirectChronicleQueue) new ChronicleQueueBuilder(file.getAbsolutePath()).build();

            final ExcerptAppender appender = chronicle.createAppender();

            appender.writeDocument(wire -> wire.write(() -> "key").text("value=" + 0));

            // creates the indexes - index's 1 and 2 are created by the indexer
            Indexer.index(chronicle);

            // create 100 documents
            for (int i = 3; i < 100; i++) {
                final int j = i;
                appender.writeDocument(wire -> wire.write(() -> "key").text("value=" + j));
            }

            final ExcerptTailer tailer = chronicle.createTailer();


            int index = 70;

            tailer.index(index);

            StringBuilder sb = new StringBuilder();
            tailer.readDocument(wire -> wire.read(() -> "key").text(sb));

            Assert.assertEquals("value=" + index, sb.toString());

        } finally {
            file.delete();
        }

    }

    @Test
    @Ignore
    public void testScanFromLastKnownIndex() throws Exception {

        File file = File.createTempFile("chronicle.", "q");
        file.deleteOnExit();
        try {

            DirectChronicleQueue chronicle = (DirectChronicleQueue) new ChronicleQueueBuilder(file.getAbsolutePath()).build();

            final ExcerptAppender appender = chronicle.createAppender();

            // create 100 documents
            for (int i = 0; i < 65; i++) {
                final int j = i;
                appender.writeDocument(wire -> wire.write(() -> "key").text("value=" + j));
            }


            // creates the indexes - index's 1 and 2 are created by the indexer
            Indexer.index(chronicle);

            // create 100 documents
            for (long i = chronicle.lastIndex() + 1; i < 200; i++) {
                final long j = i;
                appender.writeDocument(wire -> wire.write(() -> "key").text("value=" + j));
            }

            final ExcerptTailer tailer = chronicle.createTailer();


            int expected = 150;
            tailer.index(expected);

            StringBuilder sb = new StringBuilder();
            tailer.readDocument(wire -> wire.read(() -> "key").text(sb));

            Assert.assertEquals("value=" + expected, sb.toString());

        } finally {
            file.delete();
        }

    }
}