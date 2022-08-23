/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under Contract only
 */

package net.openhft.chronicle.queue.channel;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.channel.ChronicleChannelSupplier;
import net.openhft.chronicle.wire.channel.ChronicleContext;
import net.openhft.chronicle.wire.channel.InternalChronicleChannel;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class PerfThroughputMain {
    static final String URL = System.getProperty("url", "tcp://:1248");
    static final int RUN_TIME = Integer.getInteger("runTime", 5);
    static final int CLIENTS = Integer.getInteger("clients", 1);
    static final boolean METHODS = Jvm.getBoolean("methods");
    static final String PATH = System.getProperty("path", ".");

    public static void main(String[] args) {
        System.out.println("-Durl=" + URL + " " +
                "-Dpath=" + PATH + " " +
                "-DrunTime=" + RUN_TIME + " " +
                "-Dclients=" + CLIENTS + " " +
                "-Dmethods=" + METHODS);

        doTest("buffered", true);

        doTest("unbuffered", false);
    }

    private static void doTest(String desc, boolean buffered) {
        String prefix = PATH + "/q" + Long.toString(System.nanoTime(), 36) + "-";
        try (ChronicleContext context = ChronicleContext.newContext(URL)) {
            InternalChronicleChannel[] clients = new InternalChronicleChannel[CLIENTS];
            for (int i = 0; i < CLIENTS; i++) {
                PipeHandler handler = new PipeHandler()
                        .publish(prefix + i)
                        .subscribe(prefix + i)
                        .buffered(buffered);
                final ChronicleChannelSupplier supplier = context.newChannelSupplier(handler);
                clients[i] = (InternalChronicleChannel) supplier.get();
            }

            for (int size = 128 << 10; size >= 8; size /= 2) {
                long start = System.currentTimeMillis();
                long end = start + RUN_TIME * 1000L;
                int window = (4 << 20) / size / CLIENTS;
                AtomicLong totalRead = new AtomicLong(0);
                int finalSize = size;
                final Consumer<InternalChronicleChannel> sendAndReceive;

                if (METHODS) {
                    if (size < 256) {
                        // send messages via MethodWriters
                        DummyDataSmall dd = new DummyDataSmall();
                        dd.data(new byte[size - Long.BYTES]);
                        sendAndReceive = icc -> {
                            int written = 0, read = 0;
                            final EchoingSmall echoing = icc.methodWriter(EchoingSmall.class);
                            do {
                                echoing.echo(dd);

                                written += 1;

                                read = readUpto(window, icc, written, read);
                            } while (System.currentTimeMillis() < end);

                            readUpto(0, icc, written, read);
                            totalRead.addAndGet(read);
                        };
                    } else {
                        // send messages via MethodWriters
                        DummyData dd = new DummyData();
                        dd.data(new byte[size - Long.BYTES]);
                        sendAndReceive = icc -> {
                            int written = 0, read = 0;
                            final Echoing echoing = icc.methodWriter(Echoing.class);
                            do {
                                echoing.echo(dd);

                                written += 1;

                                read = readUpto(window, icc, written, read);
                            } while (System.currentTimeMillis() < end);

                            readUpto(0, icc, written, read);
                            totalRead.addAndGet(read);
                        };
                    }
                } else {
                    // send messages as raw bytes
                    sendAndReceive = icc -> {
                        int written = 0, read = 0;

                        do {
                            final Bytes<?> bytes = icc.acquireProducer().bytes();
                            bytes.writeInt(finalSize);
                            for (int i = 0; i < finalSize; i += 8)
                                bytes.writeLong(0L);
                            icc.releaseProducer();

                            // due to the multiplier in the EchoNHandler
                            written += 1;

                            read = readUpto(window, icc, written, read);
                        } while (System.currentTimeMillis() < end);

                        readUpto(0, icc, written, read);
                        totalRead.addAndGet(read);
                    };
                }
                Stream.of(clients)
                        .parallel()
                        .forEach(sendAndReceive);

                long count = totalRead.get();
                long time = System.currentTimeMillis() - start;
                long totalBytes = size * count;
                long MBps = totalBytes / time / (1_000_000 / 1_000);
                long rate = count * 1000 / time;
                System.out.printf("desc: %s, size: %,d, MBps: %,d, mps: %,d%n",
                        desc, size, MBps, rate);
            }
        }
    }

    private static int readUpto(int window, InternalChronicleChannel icc, int written, int read) {
        do {
            try (DocumentContext dc = icc.readingDocument()) {
                if (dc.isPresent())
                    read++;
            }
            Jvm.nanoPause();
        } while (written - read > window);
        return read;
    }
}
