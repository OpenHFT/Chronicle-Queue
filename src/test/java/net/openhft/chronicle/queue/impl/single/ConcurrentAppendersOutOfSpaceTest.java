/*
 * Copyright 2016-2020 Chronicle Software
 *
 * https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.wire.DocumentContext;

import java.io.File;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * This is a test scenario for manual run.
 * <p>
 * creates 2 writers, launching the first immediately
 * The 1st writer creates a queue, then loops writing to it
 * In the meantime, a background thread removes the write permissions from the cq4 queue file(s)
 * The attached writer is unaffected as the perm check has been done
 *
 * The 2nd writer thread then starts and attempts to write to the same queue
 * But this now fails the permission check
 * 2ns writer loops every seconds trying to gain access
 *
 * Eventually the background thread restores the write permission, and the 2nd thread attaches and continues normally
 *
 * Check for clean behaviour, and in particular no dangling locks left by the 2nd thread as it fails to gain
 * access to the queue while the permissions are removed
 */
public class ConcurrentAppendersOutOfSpaceTest extends QueueTestCommon {
    private static final int BLOCK_SIZE = 512 * 1024;
    private static final int MSG_SIZE = 256;
    private static final long MSGS_PER_SECOND = 1_000L;
    private static final long NANO_DELAY = 1_000_000_000L / MSGS_PER_SECOND;
    private static final AtomicInteger threadCount = new AtomicInteger(0);

    private static final String QUEUE_PATH = "concappenders";

    public static void main(String[] args) throws InterruptedException {
        final Thread writer1 = new Thread(new EndlessUpdate());
        writer1.start();

        final Thread permissionsThread = new Thread(new PermissionsManipulation());
        permissionsThread.start();

        Jvm.pause(10000);

        final Thread writer2 = new Thread(new EndlessUpdate());
        writer2.start();

        permissionsThread.join();
        writer1.join();
        writer2.join();
    }

    private static class PermissionsManipulation implements Runnable {
        @Override
        public void run() {
            System.out.println("Permissions: started");

            Jvm.pause(5000);

            File queueDir = new File(QUEUE_PATH);

            if (!queueDir.exists())
                System.out.println("Permissions: dir not found");

            if (!queueDir.isDirectory())
                System.out.println("Permissions: not a dir");

            System.out.println("Permissions: removing write permissions for queue files");

            for (File f : queueDir.listFiles()) {
                final boolean success = f.setWritable(false);

                if (!success)
                    System.out.println("Permissions: failed to remove write permissions for " + f);
            }

            Jvm.pause(10000);

            System.out.println("Permissions: adding write permissions for queue files");

            for (File f : queueDir.listFiles()) {
                final boolean success = f.setWritable(true);

                if (!success)
                    System.out.println("Permissions: failed to add write permissions for " + f);
            }
        }
    }

    private static class EndlessUpdate implements Runnable {
        @Override
        public void run() {
            int threadNo = threadCount.incrementAndGet();
            System.out.println("Writer " + threadNo + ": started");

            int messagesWritten = 0;
            byte[] sample = new byte[MSG_SIZE];
            ThreadLocalRandom r = ThreadLocalRandom.current();

            for (int i = 0; i < MSG_SIZE; i++)
                sample[i] = (byte) r.nextInt();

            for (;;) {
                try {
                    final SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(QUEUE_PATH)
                            .blockSize(BLOCK_SIZE)
                            .build();

                    try (final ExcerptAppender appender = q.acquireAppender()) {
                        for (; ; ) {
                            boolean written = true;

                            try (final DocumentContext dc = appender.writingDocument()) {
                                dc.wire().writeBytes(bytes -> {
                                    bytes.write(sample, 0, r.nextInt(8, MSG_SIZE));
                                });
                            }
                            catch (Exception e) {
                                System.out.println("Writer " + threadNo + ": failed to write message, sleeping for 1 sec");
                                e.printStackTrace();
                                Jvm.pause(1000);
                                written = false;
                            }

                            if (written)
                                messagesWritten++;

                            if (messagesWritten % MSGS_PER_SECOND == 0)
                                System.out.println("Writer " + threadNo + ": " + messagesWritten + " messages written");

                            LockSupport.parkNanos(NANO_DELAY);
                        }
                    }
                }
                catch (Exception e) {
                    System.out.println("Writer " + threadNo + ": failed to acquire appender, sleeping for 1 sec");
                    e.printStackTrace();
                    Jvm.pause(1000);
                }
            }
        }
    }

}
