package net.openhft.chronicle;

import net.openhft.chronicle.tools.ChronicleTools;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshallable;
import net.openhft.lang.model.constraints.NotNull;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;

import static org.junit.Assert.*;

public class WriteSerializableTest extends VanillaChronicleTestBase {
    @Test
    public void testWriteSerializable0() throws IOException {
        String basePath = getTestPath();
        ChronicleTools.deleteOnExit(basePath);
        Chronicle chronicle = ChronicleQueueBuilder.indexed(basePath).build();
        ExcerptAppender appender = chronicle.createAppender();
        ExcerptTailer tailer = chronicle.createTailer();
        try {
            for (int i = 0; i < 10; i++) {
                appender.startExcerpt();
                appender.writeUTFΔ("test");
                appender.writeObject(new MyData0());
                appender.finish();

                assertEquals(i + 1, appender.index());

                assertTrue(tailer.nextIndex());
                assertEquals(59, tailer.remaining());
                assertEquals("test", tailer.readUTFΔ());
                assertEquals(MyData0.class, tailer.readObject().getClass());
                tailer.finish();
            }
        } finally {
            chronicle.close();
        }
    }

    @Test
    public void testWriteSerializable() throws IOException {
        String basePath = getTestPath();
        ChronicleTools.deleteOnExit(basePath);
        Chronicle chronicle = ChronicleQueueBuilder.indexed(basePath).build();
        ExcerptAppender appender = chronicle.createAppender();
        ExcerptTailer tailer = chronicle.createTailer();
        try {
            for (int i = 0; i < 10; i++) {
                appender.startExcerpt();
                appender.writeUTFΔ("test");
                appender.writeObject(new MyData());
                appender.finish();
                try {
                    appender.finish();
                    fail();
                } catch (IllegalStateException expected) {
                }

                assertEquals(i + 1, appender.index());

                assertTrue(tailer.nextIndex());
                assertEquals(90, tailer.remaining());
                assertEquals("test", tailer.readUTFΔ());
                assertEquals(MyData.class, tailer.readObject().getClass());
                tailer.finish();
            }
        } finally {
            chronicle.close();
        }
    }

    static class MyData0 implements BytesMarshallable {
        @Override
        public void readMarshallable(@NotNull Bytes in) throws IllegalStateException {
        }

        @Override
        public void writeMarshallable(@NotNull Bytes out) {
        }
    }

    static class MyData implements Serializable {
    }
}


