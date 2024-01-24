package net.openhft.chronicle.queue.bench.interprocess;

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.bench.util.CLIUtils;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.jetbrains.annotations.NotNull;

import static net.openhft.chronicle.queue.bench.util.CLIUtils.addOption;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.single;
import static net.openhft.chronicle.queue.rollcycles.LargeRollCycles.LARGE_DAILY;

public class ProducerService implements JLBHTask {

    private static SingleChronicleQueue queue;
    private static Datum datum;
    private static int throughput;
    private static int iterations;
    private static int runs;
    private int runCounter;
    static {
        System.setProperty("disable.thread.safety", "true");
        System.setProperty("jvm.resource.tracing", "false");
        System.setProperty("check.thread.safety", "false");
    }

    private JLBH jlbh;

    public static void main(String[] args) throws InterruptedException {
        Options options = new Options();
        addOption(options, "h", "help", false, "Help", false);
        addOption(options, "p", "payload", true, "Payload Size (approximate)", false);

        addOption(options, "t", "throughput", true, "Throughput (msg/sec)", true);
        addOption(options, "i", "iterations", true, "Iterations per run", false);
        addOption(options, "r", "runs", true, "Runs", true);

        CommandLine commandLine = CLIUtils.parseCommandLine(ProducerService.class.getSimpleName(), args, options);

        datum = new Datum(CLIUtils.getIntOption(commandLine, 'p', 128));

        throughput = CLIUtils.getIntOption(commandLine, 't', 10000);
        iterations = CLIUtils.getIntOption(commandLine, 'i', throughput * 20);
        runs = CLIUtils.getIntOption(commandLine, 'r', 3);

        queue = createQueue();
//        run(datum);


        JLBHOptions lth = new JLBHOptions()
                .warmUpIterations(iterations)
                .iterations(iterations)
                .throughput(throughput)
                .recordOSJitter(commandLine.hasOption('j'))
                .accountForCoordinatedOmission(commandLine.hasOption('c'))
                .skipFirstRun(true)
                .runs(CLIUtils.getIntOption(commandLine, 'r', 3))
                .jlbhTask(new ProducerService());
        new JLBH(lth).start();
    }


    @Override
    public void init(JLBH jlbh) {
        this.jlbh = jlbh;

    }

    private boolean running = false;
    private AffinityLock coreLock;
    private ExcerptAppender appender;


    @Override
    public void run(long startTimeNS) {
        if (!running) {
            running = true;
            coreLock = AffinityLock.acquireCore();
            appender = queue.createAppender();
            System.out.println("run = " + runCounter);
        }
        long ts = System.nanoTime();
        datum.ts = ts;
        try (DocumentContext dc = appender.writingDocument()) {
            dc.wire().write("datum").marshallable(datum);
        }
        jlbh.sample(System.nanoTime() - startTimeNS);
    }

    @Override
    public void runComplete() {
        running = false;
        Closeable.closeQuietly(coreLock, appender);
        runCounter++;
    }

    @Override
    public void complete() {
        queue.close();
    }

    @NotNull
    public static SingleChronicleQueue createQueue() {
        return single("replica").rollCycle(LARGE_DAILY).doubleBuffer(false).build();
    }
}
