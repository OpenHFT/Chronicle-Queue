package net.openhft.chronicle.queue.bench.multiprocess;

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.bench.CLIUtils;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import static net.openhft.chronicle.queue.bench.CLIUtils.addOption;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.single;
import static net.openhft.chronicle.queue.rollcycles.LargeRollCycles.LARGE_DAILY;

public class ProducerService {

    private static SingleChronicleQueue queue;
    private static Datum datum;

    static {
        System.setProperty("disable.thread.safety", "true");
        System.setProperty("jvm.resource.tracing", "false");
        System.setProperty("check.thread.safety", "false");
    }

    public static void main(String[] args) {
        Options options = new Options();
        addOption(options, "h", "help", false, "Help", false);
        addOption(options, "p", "payload", true, "Payload Size (approximate)", false);
        CommandLine commandLine = CLIUtils.parseCommandLine(args, options);

        datum = new Datum(CLIUtils.getIntOption(commandLine, 'p', 128));

        queue = single("replica").rollCycle(LARGE_DAILY).doubleBuffer(false).build();
        run(datum);
    }

    private static void run(Datum datum21) {
        try (final AffinityLock ignored = AffinityLock.acquireCore();
             final ExcerptAppender app = queue.createAppender()) {
            int counter = 400_000;
            while (counter > 0) {
                counter--;
                final long start = System.nanoTime();
                datum21.ts = start;
                try (DocumentContext dc = app.writingDocument()) {
                    dc.wire().write("datum").marshallable(datum21);
                }
                if (counter % 10000 == 0) {
                    System.out.println("counter = " + counter);
                }
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            }
            queue.close();
        }
    }
}
