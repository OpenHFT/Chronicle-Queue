/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.table;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SuppressWarnings({"deprecation", "removal"})
public class SingleTableStoreIntegrationTests extends QueueTestCommon {

    private QueueContext context;

    @BeforeEach
    public void beforeEach() {
        context = new QueueContext();
    }

    @AfterEach
    public void after() throws IOException {
        context.close();
    }

    @Test
    public void baseCasePutAndGet() {
        context.newQueueInstance().tableStorePut("a", 1);
        assertEquals(1, context.newQueueInstance().tableStoreGet("a"), "table store should return value 1 for key 'a' stored in previous queue instance");
    }

    @Test
    public void getMissingKeyWithoutDefault() {
        assertEquals(Long.MIN_VALUE, context.newQueueInstance().tableStoreGet("test"), "table store should return Long.MIN_VALUE for missing key without default value");
    }

    @Test
    public void growNumberOfKeys() {
        SingleChronicleQueue queue1 = context.newQueueInstance();
        queue1.tableStorePut("a", 1);
        queue1.tableStorePut("b", 2);

        SingleChronicleQueue queue2 = context.newQueueInstance();
        queue2.tableStorePut("c", 3);

        SingleChronicleQueue queue3 = context.newQueueInstance();
        assertEquals(1, queue3.tableStoreGet("a"), "table store should return value 1 for key 'a' stored by first queue instance");
        assertEquals(2, queue3.tableStoreGet("b"), "table store should return value 2 for key 'b' stored by first queue instance");
        assertEquals(3, queue3.tableStoreGet("c"), "table store should return value 3 for key 'c' stored by second queue instance");
    }

    @Test
    public void largeNumberOfKeyValuePairs() {
        finishedNormally = false;
        SingleChronicleQueue queue1 = context.newQueueInstance();
        int count = 4_000;
        for (int i = 0; i < count; i++) {
            queue1.tableStorePut("key.prefix." + i, i);
        }
        for (int i = 0; i < count; i++) {
            assertEquals(i, queue1.tableStoreGet("key.prefix." + i), "table store should return correct value for key at index " + i + " after storing 4000 key-value pairs");
        }
        finishedNormally = true;
    }

    @Test
    public void longKeyPutAndGet() {
        SingleChronicleQueue queue1 = context.newQueueInstance();
        StringBuilder keyBuffer = new StringBuilder("AAAA");
        Random random = new Random();
        while (keyBuffer.length() < 100) {
            keyBuffer.append(random.nextInt());
        }
        String key = keyBuffer.toString();
        queue1.tableStorePut(key, 1);
        assertEquals(1, context.newQueueInstance().tableStoreGet(key), "table store should return value 1 for long key (100+ characters) stored in previous queue instance");
    }

    class QueueContext implements Closeable {

        private final File queuePath = getTmpDir();
        private final List<SingleChronicleQueue> queues = new ArrayList<>();

        /**
         * @return A fresh Queue instance pointing at the same path as all other queue instances for this test context.
         */
        SingleChronicleQueue newQueueInstance() {
            SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder().path(queuePath).build();
            queues.add(queue);
            return queue;
        }

        @Override
        public void close() {
            queues.forEach(net.openhft.chronicle.core.io.Closeable::closeQuietly);
            IOTools.deleteDirWithFiles(queuePath);
        }
    }
}
