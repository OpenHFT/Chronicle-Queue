package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.util.Time;

import java.io.File;
import java.io.FileNotFoundException;

/*
-Dtarget=/home/peter/tmp/ -Dthroughput=1000 -Dorg.slf4j.simpleLogger.defaultLogLevel=info -Druns=40
 */

@RequiredForClient
public class MultiQueueStressMain {
    static final int queueCount = Integer.getInteger("queues", 25);
    static final int throughput = Integer.getInteger("throughput", 50); // MB/s
    static final String target = System.getProperty("target", OS.getTarget());
    static final int runs = Integer.getInteger("runs", 10);

    public static void main(String[] args) throws FileNotFoundException {
        for (int t = 0; t < runs; t++) {
            long start0 = System.currentTimeMillis();
            int count = 0;
            String baseDir = target + "/deleteme/" + Time.uniqueId();
            new File(baseDir).mkdirs();
            MappedBytes[] queues = new MappedBytes[queueCount];
            int pagesPer10Second = (int) (10L * (throughput << 20) / queueCount / (4 << 10));
            for (int i = 0; i < queueCount; i++) {
                long start1 = System.currentTimeMillis();
                String filename = baseDir + "/" + count++;
                queues[i] = MappedBytes.mappedBytes(filename, pagesPer10Second * (4 << 10));
                long time1 = System.currentTimeMillis() - start1;
//                if (time1 > 20)
//                    System.out.printf("Creating %s took %.3f seconds%n", filename, time1 / 1e3);
            }
            long mid1 = System.currentTimeMillis();
            for (int i = 0; i < pagesPer10Second; i++) {
                for (MappedBytes bytes : queues) {
                    bytes.writeLong(i * (4 << 10), i);
                }
            }
            long mid2 = System.currentTimeMillis();
            for (MappedBytes bytes : queues) {
                bytes.releaseLast();
            }
            long end0 = System.currentTimeMillis();
            long time0 = end0 - start0;
            System.out.printf("Took %.3f seconds to write %,d MB, create: %.3f, write: %.3f, close: %.3f%n",
                    time0 / 1e3, throughput * 10, (mid1 - start0) / 1e3, (mid2 - mid1) / 1e3, (end0 - mid2) / 1e3);
            long delay = 10000 - time0;
            if (delay > 0)
                Jvm.pause(delay);
        }
    }
}
