package net.openhft.chronicle.tcp;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.tools.ChronicleTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.ChronicleQueueBuilder.indexed;

public class BenchmarkSource {

    public static void main(String[] args) throws InterruptedException {
        final Daemon daemon = new Daemon();
        daemon.start();
        daemon.join();
    }

    private static class Daemon extends Thread {

        private static final Logger log = LoggerFactory.getLogger(Daemon.class);

        private final int messages = 5000000;

        public Daemon() {
            setName("BenchmarkSourceDaemon");
        }

        protected synchronized String getIndexedTestPath(String suffix) {
            final File file = new File("D:/Temp/benchmarks", suffix);
            file.mkdirs();
            final String path = file.getAbsolutePath();
            ChronicleTools.deleteOnExit(path);
            return path;
        }

        @Override
        public void run() {
            final String basePathSource = getIndexedTestPath("source-" + System.nanoTime());

            try {
                final Chronicle source = indexed(basePathSource)
                        .source()
                        .bindAddress(49204)
                        .busyPeriod(10, TimeUnit.SECONDS)
                        .build();

                final ExcerptAppender excerpt = source.createAppender();
                for (int i = 1; i <= messages; i++) {
                    // use a size which will cause mis-alignment.
                    excerpt.startExcerpt();
                    excerpt.writeLong(i);
                    excerpt.append(' ');
                    excerpt.append(i);
                    excerpt.append('\n');
                    excerpt.finish();
                }
                excerpt.close();
                System.out.println(System.currentTimeMillis() + ": Finished writing messages");

                while(true) {
                    Thread.sleep(1000L);
                }
            } catch (IOException | InterruptedException e) {
                log.error("Error", e);
            }


        }
    }
}
