package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.Random;

/*
 * Created by AM on 3/20/17. Mainly for documentation: Write counterpart for FsFullRestTest.
 * Fails on Linux with a JVM SIGBUS unfortunately. Would be great if it's a "normal" runtime
 * exception.
 */
@RequiredForClient
public class FsFullWriteTest {

    /*
      as root:
       mount -o size=100M -t tmpfs none /mnt/tmpfs
       mkdir /mnt/tmpfs/cq && chown <user>:<group> /mnt/tmpfs/cq
       dd if=/dev/zero of=/mnt/tmpfs/fillspace bs=1M count=90
      */
    @NotNull
    private static String basePath = "/mnt/tmpfs/cq/testqueue";

    //@Before
    public void before() throws Exception {
        Path rootPath = Paths.get(basePath);
        Files.walk(rootPath, FileVisitOption.FOLLOW_LINKS)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .peek(System.out::println)
                .forEach(File::delete);
    }

    @Test
    public void testAppenderFullFs() throws Exception {

        ChronicleQueue queue = SingleChronicleQueueBuilder.binary(basePath)
                .blockSize(256 << 1000)
                .rollCycle(RollCycles.DAILY)
                .build();
        ExcerptAppender appender = queue.acquireAppender();
        byte[] payload = new byte[1024];
        Random r = new Random();
        r.nextBytes(payload);
        final LocalDateTime now = LocalDateTime.now(Clock.systemUTC());
        for (int i = 0; i < 1024 * 200; i++) {
            DocumentContext dc = appender.writingDocument();
            try {

                Wire w = dc.wire();
                w.write().dateTime(now);
                w.write().bytes(payload);
            } finally {
                dc.close();
            }
        }
    }

}