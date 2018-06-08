package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.threads.NamedThreadFactory;
import org.junit.Test;

import java.io.File;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

public class PreloaderStressTest extends RollCycleMultiThreadStressTest {

    @Test
    public void stress() {
        final File path = Optional.ofNullable(System.getProperty("stress.test.dir")).
                map(s -> new File(s, UUID.randomUUID().toString())).
                orElse(DirectoryUtils.tempDir("pretouchStress"));

        System.out.printf("Queue dir: %s at %s%n", path.getAbsolutePath(), Instant.now());
        final ExecutorService executorService = Executors.newFixedThreadPool(3, new NamedThreadFactory("pretouch"));

        final AtomicInteger wrote = new AtomicInteger();
        final int expectedNumberOfMessages = (int) (TEST_TIME * 1e9 / SLEEP_PER_WRITE_NANOS);

        System.out.printf("Writing %d messages with %dns interval%n", expectedNumberOfMessages, SLEEP_PER_WRITE_NANOS);
        System.out.printf("Should take ~%dsec%n", TimeUnit.NANOSECONDS.toSeconds(expectedNumberOfMessages * SLEEP_PER_WRITE_NANOS));

        final List<Future<Throwable>> results = new ArrayList<>();
        final List<Reader> readers = new ArrayList<>();
        final List<Writer> writers = new ArrayList<>();

        final PreloaderThread preloader = new PreloaderThread(path);
        executorService.submit(preloader);

        {
            final Reader reader = new Reader(path, expectedNumberOfMessages);
            readers.add(reader);
            results.add(executorService.submit(reader));

            final Writer writer = new Writer(path, wrote, expectedNumberOfMessages);
            writers.add(writer);
            results.add(executorService.submit(writer));
        }

        // TODO: dedupe with super
        final long maxWritingTime = TimeUnit.SECONDS.toMillis(MAX_WRITING_TIME);
        long startTime = System.currentTimeMillis();
        final long giveUpWritingAt = startTime + maxWritingTime;
        long nextRollTime = System.currentTimeMillis() + ROLL_EVERY_MS, nextCheckTime = System.currentTimeMillis() + 5_000;
        int i = 0;
        long now;
        while ((now = System.currentTimeMillis()) < giveUpWritingAt) {
            if (wrote.get() >= expectedNumberOfMessages)
                break;
            if (now > nextRollTime) {
                timeProvider.advanceMillis(1000);
                nextRollTime += ROLL_EVERY_MS;
            }
            if (now > nextCheckTime) {
                String readersLastRead = readers.stream().map(reader -> Integer.toString(reader.lastRead)).collect(Collectors.joining(","));
                System.out.printf("Writer has written %d of %d messages after %dms. Readers at %s. Waiting...%n",
                        wrote.get() + 1, expectedNumberOfMessages,
                        i * 10, readersLastRead);
                readers.stream().filter(r -> !r.isMakingProgress()).findAny().ifPresent(reader -> {
                    if (reader.exception != null) {
                        throw new AssertionError("Reader encountered exception, so stopped reading messages", reader.exception);
                    }
                    throw new AssertionError("Reader is stuck");
                });
                if (preloader.exception != null)
                    throw new AssertionError("Preloader encountered exception", preloader.exception);
                nextCheckTime = System.currentTimeMillis() + 10_000L;
            }
            i++;
            Jvm.pause(5);
        }
        double timeToWriteSecs = (System.currentTimeMillis() - startTime) / 1000d;

        final StringBuilder writerExceptions = new StringBuilder();
        writers.stream().filter(w -> w.exception != null).forEach(w -> {
            writerExceptions.append("Writer failed due to: ").append(w.exception.getMessage()).append("\n");
        });

        assertTrue("Wrote " + wrote.get() + " which is less than " + expectedNumberOfMessages + " within timeout. " + writerExceptions,
                wrote.get() >= expectedNumberOfMessages);

        readers.stream().filter(r -> r.exception != null).findAny().ifPresent(reader -> {
            throw new AssertionError("Reader encountered exception, so stopped reading messages",
                    reader.exception);
        });

        System.out.println(String.format("All messages written in %,.0fsecs at rate of %,.0f/sec %,.0f/sec per writer (actual writeLatency %,.0fns)",
                timeToWriteSecs, expectedNumberOfMessages / timeToWriteSecs, (expectedNumberOfMessages / timeToWriteSecs),
                1_000_000_000 / ((expectedNumberOfMessages / timeToWriteSecs))));

        final long giveUpReadingAt = System.currentTimeMillis() + 60_000L;
        final long dumpThreadsAt = giveUpReadingAt - 15_000L;
        while (System.currentTimeMillis() < giveUpReadingAt) {
            boolean allReadersComplete = areAllReadersComplete(expectedNumberOfMessages, readers);

            if (allReadersComplete) {
                break;
            }

            if (dumpThreadsAt < System.currentTimeMillis()) {
                Thread.getAllStackTraces().forEach((n, st) -> {
                    System.out.println("\n\n" + n + "\n\n");
                    Arrays.stream(st).forEach(System.out::println);
                });
            }

            System.out.printf("Not all readers are complete. Waiting...%n");
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1L));
        }
        assertTrue("Readers did not catch up",
                areAllReadersComplete(expectedNumberOfMessages, readers));

        executorService.shutdownNow();

        results.forEach(f -> {
            try {
                final Throwable exception = f.get();
                if (exception != null) {
                    exception.printStackTrace();
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });

        System.out.println("Test complete");
        DirectoryUtils.deleteDir(path);
    }

    class PreloaderThread implements Callable<Throwable> {

        final File path;
        volatile Throwable exception;

        PreloaderThread(File path) {
            this.path = path;
        }

        @Override
        public Throwable call() throws Exception {
            try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(path)
                    .testBlockSize()
                    .timeProvider(timeProvider)
                    .rollCycle(RollCycles.TEST_SECONDLY)
                    .build()) {
                ExcerptAppender appender = queue.acquireAppender();
                System.out.println("Starting pretoucher");
                while (!Thread.currentThread().isInterrupted()) {
                    Jvm.pause(50);
                    appender.pretouch();
                }
            } catch (Throwable e) {
                exception = e;
                return e;
            }
            return null;
        }
    }
}