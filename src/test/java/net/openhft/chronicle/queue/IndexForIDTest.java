package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Byteable;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.values.Array;
import net.openhft.chronicle.values.Values;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import org.junit.Test;

import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class IndexForIDTest extends QueueTestCommon {
    private int count;
    private String staged;

    private static void applyFlyweight(Facade datum, long datumSize, DocumentContext dc) {
        Wire wire = dc.wire();
        Bytes<?> bytes = wire.bytes();
        datum.bytesStore(bytes, bytes.readPosition(), datumSize);
    }

    void producer() {
        try {
            Facade datum = Values.newNativeReference(Facade.class);
            long datumSize = datum.maxSize();
            assertEquals(256, datumSize);

            Thread pretoucher = new Thread(() -> {
                try (ChronicleQueue queue = ChronicleQueue.single(staged);
                ExcerptAppender appender0 = queue.acquireAppender()) {
                    while (!Thread.currentThread().isInterrupted()) {
                        appender0.pretouch();
                        Jvm.pause(10);
                    }
                }
            });
            pretoucher.setDaemon(true);
            pretoucher.start();

            try (ChronicleQueue queue = ChronicleQueue.single(staged);
                 ExcerptAppender appender = queue.acquireAppender();
                 LongValue value = queue.indexForId("producer")) {
                // always go to the end.
                value.setOrderedValue(Long.MAX_VALUE);

                for (int i = 0; i < count; i++) {
                    try (DocumentContext dc = appender.writingDocument()) {
                        Wire wire = dc.wire();
                        Bytes<?> bytes = wire.bytes();
                        datum.bytesStore(bytes, bytes.writePosition(), datumSize);
                        bytes.writeSkip(datumSize);

                        datum.setProducerTime(System.nanoTime());
                    }
                }
                pretoucher.interrupt();
                try {
                    pretoucher.join(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    void first() {
        copy("producer", Facade::setFirstTime, "first");
    }

    void mid() {
        copy("first", Facade::setMidTime, "mid");
    }

    void end() {
        copy("mid", Facade::setEndTime, "end");
    }

    private void copy(String fromID, TimeSetter setTime, String toID) {
        Facade datum = Values.newNativeReference(Facade.class);
        long datumSize = datum.maxSize();
        long end = System.currentTimeMillis() + (Jvm.isCodeCoverage() ? 90_000 : 60_000);
        try (ChronicleQueue queue = ChronicleQueue.single(staged);
             ExcerptTailer tailer = queue.createTailer();
             LongValue fromIndex = queue.indexForId(fromID);
             LongValue toIndex = queue.indexForId(toID)) {

            for (int i = 0; i < count; i++) {
                final long index;
                try (final DocumentContext dc = tailer.readingDocument()) {
                    if (!dc.isPresent()) {
                        Jvm.pause(1);
                        i--;
                        // commented out newly introduced fail which is blowing up in TeamCity
                        // https://github.com/OpenHFT/Chronicle-Queue/issues/897
                        if (end < System.currentTimeMillis())
                            fail("Timed out i: " + i);
                        continue;
                    }
                    index = dc.index();
                    while (index > fromIndex.getVolatileValue())
                        Jvm.nanoPause();

                    applyFlyweight(datum, datumSize, dc);

                    setTime.apply(datum, System.nanoTime());

                    // write the index processed
                    toIndex.setVolatileValue(index);
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    @Test //(timeout = 10_000)
    public void staged() {
        staged = IOTools.createTempDirectory("staged").toString();
        this.count = Jvm.isArm() ? 10_000 : 1_000_000;
        Stream.<Runnable>of(
                this::producer,
                this::first,
                this::mid,
                this::end)
//                .parallel() // comment out to run sequentially
                .forEach(Runnable::run);
        IOTools.deleteDirWithFiles(staged, 3);
    }

    @FunctionalInterface
    interface TimeSetter {
        void apply(Facade facade, long time);
    }

    interface Facade extends Byteable {
        long getProducerTime();

        void setProducerTime(long timeNS);

        long getFirstTime();

        void setFirstTime(long timeNS);

        long getMidTime();

        void setMidTime(long timeNS);

        long getEndTime();

        void setEndTime(long timeNS);

        @Array(length = 28)
        double getValueAt(int index);

        void setValueAt(int index, double value);
    }
}
