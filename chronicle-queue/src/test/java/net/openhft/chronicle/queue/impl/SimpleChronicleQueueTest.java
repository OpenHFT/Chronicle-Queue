package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.WireKey;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

/**
 * @author Rob Austin.
 */
public class SimpleChronicleQueueTest {

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

        for (int j = 0; j < chronicle.lastIndex(); j++) {
            StringBuilder sb = new StringBuilder();

            tailer.readDocument(wireIn -> {
                wireIn.read(() -> "key").text(sb);
            });

            Assert.assertEquals("value=" + j, sb.toString());


        }
        ;
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

            StringBuilder sb = new StringBuilder();
            tailer.readDocument(wire -> wire.read(() -> "key").text(sb));

            Assert.assertEquals("value=5", sb.toString());


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
            Assert.assertEquals(-1, chronicle.lastIndex());

        } finally {
            file.delete();
        }

    }

    @Test
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
    public void testReadAtIndexWithIndexes() throws Exception {

        File file = File.createTempFile("chronicle.", "q");
        file.deleteOnExit();
        try {

            AbstractChronicle chronicle = new ChronicleQueueBuilder(file.getAbsolutePath())
                    .wireType(BinaryWire.class)
                    .build();

            final ExcerptAppender appender = chronicle.createAppender();

            // create 100 documents
            for (int i = 0; i < 100; i++) {
                final int j = i;
                appender.writeDocument(wire -> wire.write(() -> "key").text("value=" + j));
            }


            // creates the indexes
            new Indexer(chronicle).index();

            final ExcerptTailer tailer = chronicle.createTailer();

            tailer.index(67);

            StringBuilder sb = new StringBuilder();
            tailer.readDocument(wire -> wire.read(() -> "key").text(sb));

            Assert.assertEquals("value=67", sb.toString());

        } finally {
            file.delete();
        }

    }

    @Test
    public void testReadAtIndexWithIndexesAtStart() throws Exception {


        final File file = File.createTempFile("chronicle.", "q");
        file.deleteOnExit();

        try {

            AbstractChronicle chronicle = new ChronicleQueueBuilder(file.getAbsolutePath()).build();

            final ExcerptAppender appender = chronicle.createAppender();

            // create 100 documents
            for (int i = 0; i < 100; i++) {
                final int j = i;
                appender.writeDocument(wire -> wire.write(() -> "key").text("value=" + j));
            }


            new Indexer(chronicle).index();

            long index = 67;
            final ExcerptTailer tailer = chronicle.createTailer();
            tailer.index(index);

            //   QueueDumpMain.dump(file, new PrintWriter(System.out));

            StringBuilder sb = new StringBuilder();
            tailer.readDocument(wire -> wire.read(() -> "key").text(sb));

            Assert.assertEquals("value="+index, sb.toString());


        } finally {
            file.delete();
        }


    }

    @Test
    public void testScanFromLastKnownIndex() throws Exception {

        File file = File.createTempFile("chronicle.", "q");
        file.deleteOnExit();
        try {

            AbstractChronicle chronicle = new ChronicleQueueBuilder(file.getAbsolutePath())
                    .build();

            final ExcerptAppender appender = chronicle.createAppender();

            // create 100 documents
            for (int i = 0; i < 65; i++) {
                final int j = i;
                appender.writeDocument(wire -> wire.write(() -> "key").text("value=" + j));
            }


            // creates the indexes - index's 1 and 2 are created by the indexer
            new Indexer(chronicle).index();


            // create 100 documents
            for (long i = chronicle.lastIndex() + 1; i < 200; i++) {
                final long j = i;
                appender.writeDocument(wire -> wire.write(() -> "key").text("value=" + j));
            }

            final ExcerptTailer tailer = chronicle.createTailer();
            {
                int expected = 150;
                tailer.index(expected);

                StringBuilder sb = new StringBuilder();
                tailer.readDocument(wire -> wire.read(() -> "key").text(sb));

                Assert.assertEquals("value=" + expected, sb.toString());
            }

            //read back earlier
            {
                int expected = 167;
                tailer.index(expected);

                StringBuilder sb = new StringBuilder();
                tailer.readDocument(wire -> wire.read(() -> "key").text(sb));

                Assert.assertEquals("value=" + expected, sb.toString());
            }

        } finally {
            file.delete();
        }

    }

    enum Field implements WireKey {
        TEXT
    }
}