/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.ref.BinaryTwoLongReference;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.wire.Sequence;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.stream.Stream;

import static net.openhft.chronicle.queue.RollCycles.DEFAULT;
import static net.openhft.chronicle.queue.rollcycles.LargeRollCycles.HUGE_DAILY;
import static net.openhft.chronicle.queue.rollcycles.LegacyRollCycles.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(RollCycleEncodeSequenceTest.RollCycleEncodeSequenceTemplateProvider.class)
public class RollCycleEncodeSequenceTest extends QueueTestCommon {
    private final BinaryTwoLongReference longValue;
    private final RollCycleEncodeSequence rollCycleEncodeSequence;
    private final Bytes<ByteBuffer> bytes;

    public RollCycleEncodeSequenceTest(final RollCycle cycle) {
        longValue = new BinaryTwoLongReference();
        bytes = Bytes.elasticByteBuffer();
        longValue.bytesStore(bytes, 0, 16);
        rollCycleEncodeSequence = new RollCycleEncodeSequence(longValue, cycle.defaultIndexCount(), cycle.defaultIndexSpacing());
    }

    private static Stream<RollCycle> cases() {
        return Stream.of(DAILY, DEFAULT, HOURLY, MINUTELY, HUGE_DAILY);
    }

    static final class RollCycleEncodeSequenceTemplateProvider implements TestTemplateInvocationContextProvider {
        @Override
        public boolean supportsTestTemplate(ExtensionContext context) {
            return true;
        }

        @Override
        public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
            return cases().map(RollCycleEncodeSequenceInvocationContext::new);
        }
    }

    private static final class RollCycleEncodeSequenceInvocationContext implements TestTemplateInvocationContext {
        private final RollCycle cycle;

        private RollCycleEncodeSequenceInvocationContext(RollCycle cycle) {
            this.cycle = cycle;
        }

        @Override
        public String getDisplayName(int invocationIndex) {
            return String.valueOf(cycle);
        }

        @Override
        public java.util.List<Extension> getAdditionalExtensions() {
            return Collections.singletonList(new RollCycleEncodeSequenceParameterResolver(cycle));
        }
    }

    private static final class RollCycleEncodeSequenceParameterResolver implements ParameterResolver {
        private final RollCycle cycle;

        private RollCycleEncodeSequenceParameterResolver(RollCycle cycle) {
            this.cycle = cycle;
        }

        @Override
        public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
            return parameterContext.getParameter().getType() == RollCycle.class;
        }

        @Override
        public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
            return cycle;
        }
    }

    @Override
    public void preAfter() {
        longValue.close();
        bytes.releaseLast();
    }

    @TestTemplate
    @DisplayName("Write position maps to NOT_FOUND_RETRY")
    public void forWritePosition() {
        longValue.setOrderedValue(1);
        longValue.setOrderedValue2(2);
        // a cast to int of this magic number was causing problems
        long forWritePosition = 0x8001cc54L;
        long sequence = rollCycleEncodeSequence.getSequence(forWritePosition);
        assertEquals(Sequence.NOT_FOUND_RETRY, sequence, "writePosition: should report NOT_FOUND_RETRY");
    }

    @TestTemplate
    @DisplayName("Sequence round-trips after set and get on position")
    public void setGet() {
        int sequenceInitial = 0xb;
        int position = 0x40284;
        rollCycleEncodeSequence.setSequence(sequenceInitial, position);
        long sequence = rollCycleEncodeSequence.getSequence(position);
        assertEquals(sequenceInitial, sequence, "Sequence should round-trip for set/get position");
    }

    @TestTemplate
    @DisplayName("Sequence round-trips with masked position")
    public void setGetPositionNeedsMasking() {
        int sequenceInitial = 0xb;
        long position = 0x123456789abL;
        rollCycleEncodeSequence.setSequence(sequenceInitial, position);
        long sequence = rollCycleEncodeSequence.getSequence(position);
        assertEquals(sequenceInitial, sequence, "Sequence should round-trip for masked position");
    }

    @TestTemplate
    @DisplayName("Sequence round-trips at max masked position")
    public void setGetPositionMinus1() {
        int sequenceInitial = 0xb;
        long position = (1L << 48) - 1;
        rollCycleEncodeSequence.setSequence(sequenceInitial, position);
        long sequence = rollCycleEncodeSequence.getSequence(position);
        assertEquals(sequenceInitial, sequence, "Sequence should round-trip for max masked position");
    }
}
