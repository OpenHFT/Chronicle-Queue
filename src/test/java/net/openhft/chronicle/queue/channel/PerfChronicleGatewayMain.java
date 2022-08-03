package net.openhft.chronicle.queue.channel;

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.ClosedIORuntimeException;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import net.openhft.chronicle.threads.PauserMode;
import net.openhft.chronicle.wire.channel.ChronicleChannel;
import net.openhft.chronicle.wire.channel.ChronicleContext;
import net.openhft.chronicle.wire.channel.ChronicleGatewayMain;

import java.io.File;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;

/*
Ryzen 9 5950X with Ubuntu 21.10 run with
-Durl=tcp://localhost:1248 -Dsize=256 -Dthroughput=50000 -Diterations=1500000 -DpauseMode=balanced -Dbuffered=false
-------------------------------- SUMMARY (end to end) us -------------------------------------------
Percentile   run1         run2         run3         run4         run5      % Variation
50.0:           33.73        33.73        33.73        33.34        33.60         0.76
90.0:           35.52        35.52        35.39        34.11        35.26         2.68
99.0:           37.06        37.06        36.80        36.54        36.80         0.93
99.7:           38.98        38.46        38.08        37.44        37.95         1.79
99.9:           44.74        39.49        39.36        38.72        39.23         1.31
99.97:          46.14        44.74        44.74        39.62        40.38         7.93
99.99:        2920.45        52.54        46.40        44.86        45.76        10.24
worst:       45547.52     42139.65     42270.72     43450.37     43450.37         2.03

TODO Need to tune these worst case latencies.

-Durl=tcp://localhost:1248 -Dsize=256 -Dthroughput=500000 -Diterations=15000000 -DpauseMode=balanced -Dbuffered=true
-------------------------------- SUMMARY (end to end) us -------------------------------------------
Percentile   run1         run2         run3         run4         run5      % Variation
50.0:           50.11        50.37        50.11        50.11        50.24         0.34
90.0:           58.05        58.18        58.05        58.05        58.05         0.15
99.0:           63.94        63.81        63.68        63.68        63.81         0.13
99.7:           67.71        66.94        66.69        66.69        66.94         0.26
99.9:          132.35        69.50        69.25        69.50        69.50         0.25
99.97:       11681.79        72.58        73.86        74.88        95.87        17.63
99.99:       14204.93       871.42      2732.03      3198.98      3575.81        67.42
99.997:      16433.15      2215.94      3854.34      4251.65      4562.94        41.39
99.999:      22577.15      2379.78      4251.65      4595.71      4956.16        41.92
worst:       40960.00     42270.72     43581.44     44761.09     43581.44         3.78


-XX:+UnlockCommercialFeatures
-XX:+FlightRecorder
-XX:+UnlockDiagnosticVMOptions
-XX:+DebugNonSafepoints
-XX:StartFlightRecording=filename=recording_echo2.jfr,settings=profile
 */

public class PerfChronicleGatewayMain implements JLBHTask {

    static final int THROUGHPUT = Integer.getInteger("throughput", 50_000);
    static final int ITERATIONS = Integer.getInteger("iterations", THROUGHPUT * 30);
    static final int SIZE = Integer.getInteger("size", 256);
    static final boolean BUFFERED = Jvm.getBoolean("buffered");
    static final String URL = System.getProperty("url", "tcp://:1248");
    private static final PauserMode PAUSER_MODE = PauserMode.valueOf(System.getProperty("pauserMode", PauserMode.balanced.name()));
    private DummyData data;
    private Echoing echoing;
    private MethodReader reader;
    private Thread readerThread;
    private ChronicleChannel client, server;
    private volatile boolean complete;
    private int sent;
    private volatile int count;
    private boolean warmedUp;
    private ChronicleContext context;

    public static void main(String[] args) {
        if (!URL.contains("//:"))
            System.out.println("Make sure " + ChronicleGatewayMain.class + " is running first");
        System.out.println("" +
                "-Durl=" + URL + " " +
                "-Dsize=" + SIZE + " " +
                "-Dthroughput=" + THROUGHPUT + " " +
                "-Diterations=" + ITERATIONS + " " +
                "-DpauseMode=" + PAUSER_MODE + " " +
                "-Dbuffered=" + BUFFERED);

        JLBHOptions lth = new JLBHOptions()
                .warmUpIterations(50_000)
                .iterations(ITERATIONS)
                .throughput(THROUGHPUT)
                .acquireLock(AffinityLock::acquireLock)
                // disable as otherwise single GC event skews results heavily
                .recordOSJitter(false)
                .accountForCoordinatedOmission(false)
                .runs(5)
                .jlbhTask(new PerfChronicleGatewayMain());
        new JLBH(lth).start();
    }

    @Override
    public void init(JLBH jlbh) {
        this.data = new DummyData();
        this.data.data(new byte[SIZE - Long.BYTES]);

        String path = new File("/dev/shm").isDirectory() ? "/dev/shm/echo" : OS.TMP + "/echo";
        IOTools.deleteDirWithFiles(path);
        new File(path).mkdir();

        context = ChronicleContext.newContext(URL);

        final PipeHandler handler1 = new PipeHandler().subscribe(path + "/in").publish(path + "/out").buffered(BUFFERED);
        server = context.newChannelSupplier(handler1).buffered(BUFFERED).pauserMode(PAUSER_MODE).get();
        Thread serverThread = new Thread(() -> runServer(server, Echoed.class, EchoingMicroservice::new), "server");
        serverThread.setDaemon(true);
        serverThread.start();

        final PipeHandler handler2 = new PipeHandler().publish(path + "/in").subscribe(path + "/out").buffered(BUFFERED);
        client = context.newChannelSupplier(handler2).buffered(BUFFERED).pauserMode(PAUSER_MODE).get();

        echoing = client.methodWriter(Echoing.class);
        reader = client.methodReader((Echoed) data -> {
            jlbh.sample(System.nanoTime() - data.timeNS());
            count++;
        });
        readerThread = new Thread(() -> {
            try (AffinityLock lock = AffinityLock.acquireLock()) {
                while (!Thread.currentThread().isInterrupted()) {
                    reader.readOne();
                }
            } catch (Throwable t) {
                if (!complete)
                    t.printStackTrace();
            }
        }, "last-reader");
        readerThread.setDaemon(true);
        readerThread.start();
    }

    private <OUT, MS> void runServer(ChronicleChannel server, Class<OUT> outClass, Function<OUT, MS> serviceConstructor) {
        final OUT out = server.methodWriter(outClass);
        final MS ms = serviceConstructor.apply(out);
        MethodReader reader = server.methodReader(ms);
        try (AffinityLock lock = AffinityLock.acquireLock()) {
            while (!server.isClosed()) {
                reader.readOne();
            }
        } catch (ClosedIORuntimeException closed) {
            Jvm.warn().on(getClass(), closed.toString());
        }
    }

    @Override
    public void warmedUp() {
        JLBHTask.super.warmedUp();
        warmedUp = true;
    }

    @Override
    public void run(long startTimeNS) {
        data.timeNS(startTimeNS);
        echoing.echo(data);

        // throttling when warming up.
        if (!warmedUp) {
            long lag = sent++ - count;
            if (lag >= 50)
                LockSupport.parkNanos(lag * 500L);
        }
    }

    @Override
    public void complete() {
        this.complete = true;
        readerThread.interrupt();
        client.close();
        server.close();
        context.close();
    }
}

