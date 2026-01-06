/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.core.util.ObjectUtils;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static net.openhft.chronicle.queue.rollcycles.LegacyRollCycles.HOURLY;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;
import static org.junit.jupiter.api.Assertions.*;

public class RollCycleDefaultingTest extends QueueTestCommon {

    private static final String BASE_PATH = OS.getTarget() + "/rollCycleDefaultingTest";

    @Test
    @DisplayName("RollCycle alias resolves to RollCycles implementation")
    public void alias() {
        assertEquals(RollCycles.class, ObjectUtils.implementationToUse(RollCycle.class),
                "RollCycle should resolve to default RollCycles implementation");
    }

    @AfterEach
    public void clearDefaultRollCycleProperty() {
        System.clearProperty(QueueSystemProperties.DEFAULT_ROLL_CYCLE_PROPERTY);
    }

    @AfterAll
    public static void afterClass() {
        IOTools.deleteDirWithFiles(BASE_PATH, 2);
    }

    @Test
    @DisplayName("Configured roll cycle class name loads")
    public void correctConfigGetsLoaded() {
        String aClass = HOURLY.getClass().getName();
        String configuredCycle = aClass + ":HOURLY";
        System.setProperty(QueueSystemProperties.DEFAULT_ROLL_CYCLE_PROPERTY, configuredCycle);
        SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.binary(BASE_PATH);
        assertEquals(HOURLY, builder.rollCycle(), "Configured roll cycle should resolve to HOURLY");
    }

    @Test
    @DisplayName("Custom roll cycle class loads from configuration")
    public void customDefinitionGetsLoaded() {
        String configuredCycle = MyRollcycle.class.getName();
        System.setProperty(QueueSystemProperties.DEFAULT_ROLL_CYCLE_PROPERTY, configuredCycle);
        SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.binary(BASE_PATH);

        assertInstanceOf(MyRollcycle.class, builder.rollCycle(), "Configured roll cycle should use MyRollcycle");
    }

    @Test
    @DisplayName("Unknown roll cycle defaults to daily")
    public void unknownClassDefaultsToDaily() {
        expectException("Default roll cycle class: foobarblah was not found");
        String configuredCycle = "foobarblah";
        System.setProperty(QueueSystemProperties.DEFAULT_ROLL_CYCLE_PROPERTY, configuredCycle);
        SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.binary(BASE_PATH);
        assertEquals(RollCycles.DEFAULT, builder.rollCycle(), "Unknown class should default to RollCycles.DEFAULT");

    }

    @Test
    @DisplayName("Non RollCycle class defaults to daily roll cycle")
    public void nonRollCycleDefaultsToDaily() {
        expectException("Configured default rollcycle is not a subclass of RollCycle");
        String configuredCycle = String.class.getName();
        System.setProperty(QueueSystemProperties.DEFAULT_ROLL_CYCLE_PROPERTY, configuredCycle);
        SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.binary(BASE_PATH);
        assertEquals(RollCycles.DEFAULT, builder.rollCycle(), "Non RollCycle class should default to RollCycles.DEFAULT");
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
        public net.openhft.chronicle.queue.rollcycles.RollCycleArithmetic arithmetic() {
            return delegate.arithmetic();
        }

        @Override
        public long maxMessagesPerCycle() {
            return 0;
        }
    }
}
