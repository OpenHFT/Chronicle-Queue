package vanilla.java.echo;

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;

import java.io.IOException;

public class QueueServerMain {
    public static void main(String... args) throws IOException {
        String host = args[0];
        if (!host.equals("localhost"))
            AffinityLock.acquireLock();

        Chronicle inbound = ChronicleQueueBuilder
                .indexed("/tmp/server-inbound")
                .sink().connectAddress(host, 54001)
                .build();
        ExcerptTailer tailer = inbound.createTailer().toEnd();

        Chronicle outbound = ChronicleQueueBuilder
                .indexed("/tmp/server-outbound")
                .source().bindAddress(54002)
                .build();

        ExcerptAppender appender = outbound.createAppender();

        long count = 0, next = 1000000;
        while (true) {
            if (tailer.nextIndex()) {
                appender.startExcerpt();
                appender.write(tailer);
                appender.finish();
                count++;
/*
                System.out.print(".");
                if ((count & 127) == 0)
                    System.out.println();
*/
            } else {
                if (count >= next)
                    System.out.println(count);
                next += 1000000;
            }
        }
    }
}
