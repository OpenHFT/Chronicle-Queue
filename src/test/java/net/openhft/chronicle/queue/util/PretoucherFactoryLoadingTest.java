/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.util;

import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.Pretoucher;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class PretoucherFactoryLoadingTest extends QueueTestCommon {

    static final Pretoucher DELEGATED = new Pretoucher() {
        @Override
        public void execute() {
        }

        @Override
        public void close() {
        }
    };

    @Before
    public void ignoreOpenSourceStartupWarning() {
        // PretouchUtil's class initialiser warns once when Enterprise is absent
        ignoreException("Pretoucher is only supported");
    }

    @Test
    public void absentEnterpriseFactoryFallsBackToClearError() {
        final PretoucherFactory factory = PretouchUtil.loadFactory("software.chronicle.enterprise.queue.pretoucher.Absent");
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir()).build()) {
            final UnsupportedOperationException e = assertThrows(UnsupportedOperationException.class, () -> factory.createPretoucher(q));
            assertTrue(e.getMessage(), e.getMessage().contains("not on the classpath"));
            assertNull(e.getCause());
        }
    }

    @Test
    public void presentEnterpriseFactoryIsDelegatedTo() {
        final PretoucherFactory factory = PretouchUtil.loadFactory(StubEnterpriseFactory.class.getName());
        assertTrue(factory instanceof StubEnterpriseFactory);
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir()).build()) {
            assertSame(DELEGATED, factory.createPretoucher(q));
        }
    }

    @Test
    public void incompatibleEnterpriseFactoryIsReportedWithItsCause() {
        expectException("is present but could not be loaded");
        final PretoucherFactory factory = PretouchUtil.loadFactory(NotAFactory.class.getName());
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir()).build()) {
            final UnsupportedOperationException e = assertThrows(UnsupportedOperationException.class, () -> factory.createPretoucher(q));
            assertTrue(e.getMessage(), e.getMessage().contains("could not be loaded"));
            assertTrue(String.valueOf(e.getCause()), e.getCause() instanceof ClassCastException);
        }
    }

    public static final class StubEnterpriseFactory implements PretoucherFactory {
        @Override
        public EventHandler createEventHandler(@NotNull SingleChronicleQueue queue) {
            return () -> false;
        }

        @Override
        public Pretoucher createPretoucher(@NotNull SingleChronicleQueue queue) {
            return DELEGATED;
        }
    }

    public static final class NotAFactory {
    }
}
