package net.openhft.chronicle.tcp;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.tools.ChronicleTools;
import net.openhft.lang.io.StopCharTesters;
import org.junit.rules.TemporaryFolder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import static net.openhft.chronicle.ChronicleQueueBuilder.indexed;
import static org.junit.Assert.assertEquals;

@State(Scope.Benchmark)
public class SinkBenchmark {

    private static final Logger log = LoggerFactory.getLogger(SinkBenchmark.class);

    private final TemporaryFolder folder = new TemporaryFolder(new File(System.getProperty("java.io.tmpdir")));

    private int run = 0;

    // TODO, make more robust.
    private final int messages = 1000000;

    @Setup
    public void init() throws IOException {
        folder.create();
    }

    @Benchmark
    public int benchmark() throws InterruptedException, IOException {
        run++;
        final String basePathSink = getIndexedTestPath("sink-" + run);

        final int port = 49204;
        final Chronicle sink = indexed(basePathSink)
                .sink()
                .connectAddress("localhost", port)
                .build();

        ExcerptTailer excerpt = sink.createTailer().toStart();
        int i;
        for (i = 1; i <= messages; i++) {
            while (!excerpt.nextIndex());

            long n = excerpt.readLong();
/*
            String text = excerpt.parseUTF(StopCharTesters.CONTROL_STOP);
            if (i != n) {
                assertEquals('\'' + text + '\'', i, n);
            }
*/

            excerpt.finish();
        }

        excerpt.close();
        sink.close();

        return port | i;
    }

    private synchronized String getIndexedTestPath(String suffix) {
        final String path = getTmpDir(suffix);
        ChronicleTools.deleteOnExit(path);
        return path;
    }

    private String getTmpDir(String name) {
        try {
            String mn = "benchmarkWrite";
            File path = mn == null
                    ? folder.newFolder(getClass().getSimpleName(), name)
                    : folder.newFolder(getClass().getSimpleName(), mn, name);

            log.debug("tmpdir: " + path);

            return path.getAbsolutePath();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

}
