/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.core.util.ObjectUtils;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;

import static net.openhft.chronicle.queue.rollcycles.LegacyRollCycles.HOURLY;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RollCycleDefaultingTest extends QueueTestCommon {

    private static final String BASE_PATH = OS.getTarget() + "/rollCycleDefaultingTest";

    @Test
    public void alias() {
        assertEquals(RollCycles.class, ObjectUtils.implementationToUse(RollCycle.class));
    }

    @After
    public void clearDefaultRollCycleProperty() {
        System.clearProperty(QueueSystemProperties.DEFAULT_ROLL_CYCLE_PROPERTY);
    }

    @AfterClass
    public static void afterClass() {
        IOTools.deleteDirWithFiles(BASE_PATH, 2);
    }

    @Test
    public void correctConfigGetsLoaded() {
        String aClass = HOURLY.getClass().getName();
        String configuredCycle = aClass + ":HOURLY";
        System.setProperty(QueueSystemProperties.DEFAULT_ROLL_CYCLE_PROPERTY, configuredCycle);
        SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.binary(BASE_PATH);
        assertEquals(HOURLY, builder.rollCycle());
    }

    @Test
    public void customDefinitionGetsLoaded() {
        String configuredCycle = MyRollcycle.class.getName();
        System.setProperty(QueueSystemProperties.DEFAULT_ROLL_CYCLE_PROPERTY, configuredCycle);
        SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.binary(BASE_PATH);

        assertTrue(builder.rollCycle() instanceof MyRollcycle);
    }

    @Test
    public void unknownClassDefaultsToDaily() {
        expectException("Default roll cycle class: foobarblah was not found");
        String configuredCycle = "foobarblah";
        System.setProperty(QueueSystemProperties.DEFAULT_ROLL_CYCLE_PROPERTY, configuredCycle);
        SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.binary(BASE_PATH);
        assertEquals(RollCycles.DEFAULT, builder.rollCycle());

    }

    @Test
    public void nonRollCycleDefaultsToDaily() {
        expectException("Configured default rollcycle is not a subclass of RollCycle");
        String configuredCycle = String.class.getName();
        System.setProperty(QueueSystemProperties.DEFAULT_ROLL_CYCLE_PROPERTY, configuredCycle);
        SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.binary(BASE_PATH);
        assertEquals(RollCycles.DEFAULT, builder.rollCycle());
    }

    public static class MyRollcycle implements RollCycle {
        private final RollCycle delegate = TEST_SECONDLY;

        @Override
        public @NotNull String format() {
            return "xyz";
        }

        @Override
        public int lengthInMillis() {
            return delegate.lengthInMillis();
        }

        @Override
        public int defaultIndexCount() {
            return delegate.defaultIndexCount();
        }

        @Override
        public int defaultIndexSpacing() {
            return delegate.defaultIndexSpacing();
        }

        @Override
        public int current(TimeProvider time, long epoch) {
            return delegate.current(time, epoch);
        }

        @Override
        public long toIndex(int cycle, long sequenceNumber) {
            return delegate.toIndex(cycle, sequenceNumber);
        }

        @Override
        public long toSequenceNumber(long index) {
            return delegate.toSequenceNumber(index);
        }

        @Override
        public int toCycle(long index) {
            return delegate.toCycle(index);
        }

        @Override
        public long maxMessagesPerCycle() {
            return 0;
        }
    }
}
