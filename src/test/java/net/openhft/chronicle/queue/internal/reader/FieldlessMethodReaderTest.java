/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.reader;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.WireType;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@RunWith(Parameterized.class)
public class FieldlessMethodReaderTest extends QueueTestCommon {

    private final CustomEnumType enumType;
    private final AtomicInteger msgCounter = new AtomicInteger();

    public FieldlessMethodReaderTest(CustomEnumType enumType) {
        this.enumType = enumType;
    }

    @Test
    public void test() throws InterruptedException {
        File path = new File(getTmpDir(), "enum_test_" + enumType);

        try (SingleChronicleQueue chronicle = SingleChronicleQueueBuilder.builder().path(path)
                .wireType(WireType.FIELDLESS_BINARY).rollCycle(TestRollCycles.TEST_DAILY).build()) {
            EntityListener writer = chronicle.methodWriter(EntityListener.class);
            MethodReader methodReader = chronicle.createTailer().toEnd().methodReader((EntityListener) value -> msgCounter.incrementAndGet());

            CustomEntity entity = new CustomEntity();
            IntStream.range(0, 2).forEach(i -> writer.onMessage(entity.enumType(enumType)));

            //noinspection StatementWithEmptyBody
            while (methodReader.readOne()) {
            }
            Assert.assertEquals(2, msgCounter.get());
        } finally {
            IOTools.deleteDirWithFilesOrWait(1000, path);
        }
    }

    @Parameterized.Parameters
    public static Collection<CustomEnumType> enums() {
        return Stream.concat(Stream.of((CustomEnumType) null), Arrays.stream(CustomEnumType.values()))
                .collect(Collectors.toList());
    }

    public enum CustomEnumType {
        A,
        AA,
        AAA,
        AAAA,
        AAAAA,
        AAAAAA,
        AAAAAAA
    }

    static class CustomEntity extends SelfDescribingMarshallable {
        private CustomEnumType enumType;

        CustomEntity enumType(CustomEnumType enumType) {
            this.enumType = enumType;
            return this;
        }
    }

    interface EntityListener {
        void onMessage(CustomEntity value);
    }
}
