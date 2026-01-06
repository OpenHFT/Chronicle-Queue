/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.reader;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.WireType;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(FieldlessMethodReaderTest.FieldlessMethodReaderTemplateProvider.class)
public class FieldlessMethodReaderTest extends QueueTestCommon {

    private final CustomEnumType enumType;
    private final AtomicInteger msgCounter = new AtomicInteger();

    public FieldlessMethodReaderTest(CustomEnumType enumType) {
        this.enumType = enumType;
    }

    private static Stream<CustomEnumType> cases() {
        return Stream.concat(Stream.of((CustomEnumType) null), Arrays.stream(CustomEnumType.values()));
    }

    static final class FieldlessMethodReaderTemplateProvider implements TestTemplateInvocationContextProvider {
        @Override
        public boolean supportsTestTemplate(ExtensionContext context) {
            return true;
        }

        @Override
        public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
            return cases().map(FieldlessMethodReaderInvocationContext::new);
        }
    }

    private static final class FieldlessMethodReaderInvocationContext implements TestTemplateInvocationContext {
        private final CustomEnumType enumType;

        private FieldlessMethodReaderInvocationContext(CustomEnumType enumType) {
            this.enumType = enumType;
        }

        @Override
        public String getDisplayName(int invocationIndex) {
            return "enumType=" + enumType;
        }

        @Override
        public java.util.List<Extension> getAdditionalExtensions() {
            return Collections.singletonList(new FieldlessMethodReaderParameterResolver(enumType));
        }
    }

    private static final class FieldlessMethodReaderParameterResolver implements ParameterResolver {
        private final CustomEnumType enumType;

        private FieldlessMethodReaderParameterResolver(CustomEnumType enumType) {
            this.enumType = enumType;
        }

        @Override
        public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
            return parameterContext.getParameter().getType() == CustomEnumType.class;
        }

        @Override
        public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
            return enumType;
        }
    }

    @TestTemplate
    @DisplayName("Fieldless method reader counts all messages")
    public void test() {
        File path = new File(getTmpDir(), "enum_test_" + enumType);

        try (SingleChronicleQueue chronicle = SingleChronicleQueueBuilder.builder().path(path)
                .wireType(WireType.FIELDLESS_BINARY).rollCycle(TestRollCycles.TEST_DAILY).build()) {
            EntityListener writer = chronicle.methodWriter(EntityListener.class);
            MethodReader methodReader = chronicle.createTailer().toEnd().methodReader((EntityListener) value -> msgCounter.incrementAndGet());

            CustomEntity entity = new CustomEntity();
            IntStream.range(0, 2).forEach(i -> writer.onMessage(entity.enumType(enumType)));

            while (methodReader.readOne()) {
                Jvm.nanoPause();
            }
            assertEquals(2, msgCounter.get(), "methodReader should read expected message count");
        } finally {
            IOTools.deleteDirWithFilesOrWait(1000, path);
        }
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

        CustomEntity enumType(CustomEnumType enumType) {
            return this;
        }
    }

    interface EntityListener {
        void onMessage(CustomEntity value);
    }
}
