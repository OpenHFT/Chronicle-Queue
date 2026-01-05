/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.TestKey;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

import java.util.Collections;
import java.util.stream.Stream;

import static net.openhft.chronicle.queue.impl.single.StoreTailer.INDEXING_LINEAR_SCAN_THRESHOLD;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SuppressWarnings({"deprecation", "removal"})
@ExtendWith(IndexTest.IndexTestTemplateProvider.class)
@SuppressWarnings({"deprecation", "removal"})
public class IndexTest extends QueueTestCommon {

    @NotNull
    private final WireType wireType;

    /**
     * @param wireType the type of the wire
     */
    public IndexTest(@NotNull WireType wireType) {
        this.wireType = wireType;
    }

    private static Stream<WireType> cases() {
        // TEXT mode not supported here due to missing CAS in LongArrayReference.
        return Stream.of(WireType.BINARY);
    }

    static final class IndexTestTemplateProvider implements TestTemplateInvocationContextProvider {
        @Override
        public boolean supportsTestTemplate(ExtensionContext context) {
            return true;
        }

        @Override
        public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
            return cases().map(IndexTestInvocationContext::new);
        }
    }

    private static final class IndexTestInvocationContext implements TestTemplateInvocationContext {
        private final WireType wireType;

        private IndexTestInvocationContext(WireType wireType) {
            this.wireType = wireType;
        }

        @Override
        public String getDisplayName(int invocationIndex) {
            return String.valueOf(wireType);
        }

        @Override
        public java.util.List<Extension> getAdditionalExtensions() {
            return Collections.singletonList(new IndexTestParameterResolver(wireType));
        }
    }

    private static final class IndexTestParameterResolver implements ParameterResolver {
        private final WireType wireType;

        private IndexTestParameterResolver(WireType wireType) {
            this.wireType = wireType;
        }

        @Override
        public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
            return parameterContext.getParameter().getType() == WireType.class;
        }

        @Override
        public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
            return wireType;
        }
    }

    @TestTemplate
    public void test() {
        try (final RollingChronicleQueue queue = SingleChronicleQueueBuilder
                .binary(getTmpDir())
                .testBlockSize()
                .wireType(this.wireType)
                .build();
             final ExcerptAppender appender = queue.createAppender()) {

            for (int i = 0; i < 5; i++) {
                final int n = i;
                appender.writeDocument(
                        w -> w.write(TestKey.test).int32(n));
                final int cycle = queue.lastCycle();
                long index0 = queue.rollCycle().toIndex(cycle, n);
                long indexA = appender.lastIndexAppended();
                assertEquals(index0, indexA, "index: cycle=" + cycle + " seq=" + n);
            }
        }
    }

    @TestTemplate
    public void shouldShortCircuitIndexLookupWhenNewIndexIsCloseToPreviousIndex() {
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder
                .binary(getTmpDir())
                .testBlockSize()
                .wireType(this.wireType)
                .build();
             final ExcerptAppender appender = queue.createAppender()) {

            final int messageCount = INDEXING_LINEAR_SCAN_THRESHOLD + 5;
            final long[] indices = new long[messageCount];
            for (int i = 0; i < messageCount; i++) {
                try (final DocumentContext ctx = appender.writingDocument()) {
                    ctx.wire().write("event").int32(i);
                    indices[i] = ctx.index();
                }
            }

            final StoreTailer tailer =
                    (StoreTailer) queue.createTailer();
            tailer.moveToIndex(indices[0]);

            assertEquals(indices[0], tailer.index(), "tailer should be at index[0] after initial move");
            assertEquals(1, tailer.getIndexMoveCount(), "index move count should be 1 after first moveToIndex");

            tailer.moveToIndex(indices[0]);
            assertEquals(indices[0], tailer.index(), "tailer should remain at index[0] when moving to same position");
            assertEquals(1, tailer.getIndexMoveCount(), "index move count should stay 1 when moving to current position");

            tailer.moveToIndex(indices[2]);
            assertEquals(indices[2], tailer.index(), "tailer should advance to index[2] via linear scan");
            assertEquals(1, tailer.getIndexMoveCount(), "index move count should stay 1 for nearby forward move within threshold");

            tailer.moveToIndex(indices[INDEXING_LINEAR_SCAN_THRESHOLD + 2]);
            assertEquals(indices[INDEXING_LINEAR_SCAN_THRESHOLD + 2], tailer.index(), "tailer should jump to index beyond threshold");
            assertEquals(2, tailer.getIndexMoveCount(), "index move count should increment to 2 for move beyond linear scan threshold");

            // document that moving backwards requires an index scan
            tailer.moveToIndex(indices[INDEXING_LINEAR_SCAN_THRESHOLD - 1]);
            assertEquals(indices[INDEXING_LINEAR_SCAN_THRESHOLD - 1], tailer.index(), "tailer should move backwards to index just below threshold");
            assertEquals(3, tailer.getIndexMoveCount(), "index move count should increment to 3 for backward move requiring index scan");
        }
    }

}
