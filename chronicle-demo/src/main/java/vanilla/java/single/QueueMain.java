package vanilla.java.single;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;

import java.io.File;
import java.io.IOException;
import java.util.Date;

public class QueueMain {
    public static void main(String... ignored) throws IOException {
        Chronicle chronicle = ChronicleQueueBuilder.vanilla(new File("my-queue8")).build();

        ExcerptAppender appender = chronicle.createAppender();
        appender.startExcerpt();
        appender.writeLong(System.currentTimeMillis());
        appender.writeUTFΔ("Hello World");
        appender.finish();

        ExcerptTailer tailer = chronicle.createTailer().toStart();
        StringBuilder msg = new StringBuilder();
        while (tailer.nextIndex()) {
            long time = tailer.readLong();
            tailer.readUTFΔ(msg);
            tailer.finish();

            System.out.println(new Date(time) + " - " + msg);

            System.out.println(new Date(time) + " - " + msg);
        }
        chronicle.close();
    }
}
