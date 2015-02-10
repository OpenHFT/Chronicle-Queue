package vanilla.java.echo;

import net.openhft.affinity.AffinityLock;
import net.openhft.affinity.AffinitySupport;
import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;

import java.io.IOException;
import java.util.Arrays;

/**
 * NOTE: This test requires at least SIX (6) cpus, or the performance will be poor.
 */
public class QueueClientMain {
    static final int TESTS = Integer.getInteger("tests", 6);
    static final int RATE = Integer.getInteger("rate", 30000);
    static final int COUNT = Integer.getInteger("count", RATE * 10);
    static final long END_OF_TEST = -1L;
    static final long END_OF_TESTS = -2L;

    public static void main(String... args) throws IOException {
        String host = args[0];

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                AffinityLock lock = null;
                try {
                    Chronicle outbound = ChronicleQueueBuilder
                            .indexed("/tmp/client-outbound")
                            .source().bindAddress(54001)
                            .build();
                    lock = AffinitySupport.acquireLock();
                    ExcerptAppender appender = outbound.createAppender();
                    for (int i = 0; i < TESTS; i++) {

                        long now = System.nanoTime();
                        long spacing = 1000000000 / RATE;
                        for (int j = 0; j < COUNT; j++) {
                            while (now >= System.nanoTime()) {
                                // busy wait.
                            }
                            appender.startExcerpt();
                            appender.writeLong(System.nanoTime()); // when it should have sent the message.
                            appender.finish();
                            now += spacing;
/*
                            System.out.print("+");
                            if ((j & 127) == 0)
                                System.out.println();
*/

                        }
                        appender.startExcerpt();
                        appender.writeLong(END_OF_TEST);
                        appender.finish();
                        Thread.sleep(1000);
                    }
                    appender.startExcerpt();
                    appender.writeLong(END_OF_TESTS);
                    appender.finish();
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(-1);
                } finally {
                    if (lock != null)
                        lock.release();
                }
            }
        });
        t.start();

        Chronicle inbound = ChronicleQueueBuilder
                .indexed("/tmp/client-inbound")
                .sink().connectAddress(host, 54002)
                .build();
        ExcerptTailer tailer = inbound.createTailer().toEnd();
        long[] times = new long[COUNT];
        int count = 0, next = 1000000;

        AffinitySupport.acquireLock();
        System.out.print("Warmup - ");
        while (true) {
            if (tailer.nextIndex()) {
                long timestamp = tailer.readLong();
                tailer.finish();
                if (timestamp == END_OF_TESTS)
                    break;
                if (timestamp == END_OF_TEST) {
                    Arrays.sort(times);
                    System.out.printf("Latencies 50 90/99 99.9/99.99 %%tile %,d %,d/%,d %,d/%,d%n",
                            times[times.length - times.length / 2] / 1000,
                            times[times.length - times.length / 10] / 1000,
                            times[times.length - times.length / 100] / 1000,
                            times[times.length - times.length / 1000 - 1] / 1000,
                            times[times.length - times.length / 10000 - 1] / 1000
                    );
                    count = 0;
                } else if (count < times.length) {
                    times[count++] = System.nanoTime() - timestamp;
/*                    System.out.print(".");
                    if ((count & 127) == 0)
                        System.out.println();*/
                }
            } else if (count >= next) {
                System.out.println(count);
                next += 1000000;
            }
        }
    }
}
