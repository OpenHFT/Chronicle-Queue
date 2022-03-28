package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AppenderListenerTest {

    @Test
    public void appenderListenerTest() {
        String path = OS.getTarget() + "/appenderListenerTest";
        StringBuilder results = new StringBuilder();
        try (ChronicleQueue q = SingleChronicleQueueBuilder.single(path)
                .testBlockSize()
                .appenderListener((wire, index) -> {
                    long offset = ((index >>> 32) << 40) | wire.bytes().readPosition();
                    String event = wire.readEvent(String.class);
                    String text = wire.getValueIn().text();
                    results.append(event)
                            .append(" ").append(text)
                            .append(", addr:").append(Long.toHexString(offset))
                            .append(", index: ").append(Long.toHexString(index)).append("\n");
                })
                .timeProvider(new SetTimeProvider("2021/11/29T13:53:59").advanceMillis(1000))
                .build();
             ExcerptAppender appender = q.acquireAppender()) {
            final HelloWorld writer = appender.methodWriter(HelloWorld.class);
            writer.hello("G'Day");
            writer.hello("Bye-now");
        }
        IOTools.deleteDirWithFiles(path);
        assertEquals("" +
                "hello G'Day, addr:4a100000010114, index: 4a1000000000\n" +
                "hello Bye-now, addr:4a100000010128, index: 4a1000000001\n", results.toString());
    }

    public interface HelloWorld {
        void hello(String s);
    }
}