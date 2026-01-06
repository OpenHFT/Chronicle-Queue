/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

import java.text.ParseException;
import java.util.*;
import java.util.stream.Stream;

import static net.openhft.chronicle.queue.rollcycles.LegacyRollCycles.DAILY;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@RequiredForClient
@ExtendWith(SparseBinarySearchTest.SparseBinarySearchTemplateProvider.class)
public class SparseBinarySearchTest extends QueueTestCommon {

    private static final GapTolerantComparator GAP_TOLERANT_COMPARATOR = new GapTolerantComparator();

    private final int numberOfMessages;
    private final float percentageWithValues;

    public SparseBinarySearchTest(int numberOfMessages, float percentageWithValues) {
        this.numberOfMessages = numberOfMessages;
        this.percentageWithValues = percentageWithValues;
    }

    private static Stream<SparseBinarySearchCase> cases() {
        List<SparseBinarySearchCase> parameters = new ArrayList<>();
        List<Integer> numbersOfMessages = Arrays.asList(0, 1, 2, 100);
        List<Float> percentagesWithValues = Arrays.asList(0.0f, 0.1f, 0.9f);

        for (int nom : numbersOfMessages) {
            for (float pwv : percentagesWithValues) {
                parameters.add(new SparseBinarySearchCase(nom, pwv));
            }
        }
        return parameters.stream();
    }

    private static final class SparseBinarySearchCase {
        private final int numberOfMessages;
        private final float percentageWithValues;

        private SparseBinarySearchCase(int numberOfMessages, float percentageWithValues) {
            this.numberOfMessages = numberOfMessages;
            this.percentageWithValues = percentageWithValues;
        }
    }

    static final class SparseBinarySearchTemplateProvider implements TestTemplateInvocationContextProvider {
        @Override
        public boolean supportsTestTemplate(ExtensionContext context) {
            return true;
        }

        @Override
        public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
            return cases().map(SparseBinarySearchInvocationContext::new);
        }
    }

    private static final class SparseBinarySearchInvocationContext implements TestTemplateInvocationContext {
        private final SparseBinarySearchCase testCase;

        private SparseBinarySearchInvocationContext(SparseBinarySearchCase testCase) {
            this.testCase = testCase;
        }

        @Override
        public String getDisplayName(int invocationIndex) {
            return "items=" + testCase.numberOfMessages + ", percentage=" + testCase.percentageWithValues;
        }

        @Override
        public java.util.List<Extension> getAdditionalExtensions() {
            return Collections.singletonList(new SparseBinarySearchParameterResolver(testCase));
        }
    }

    private static final class SparseBinarySearchParameterResolver implements ParameterResolver {
        private final SparseBinarySearchCase testCase;

        private SparseBinarySearchParameterResolver(SparseBinarySearchCase testCase) {
            this.testCase = testCase;
        }

        @Override
        public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
            Class<?> type = parameterContext.getParameter().getType();
            return type == int.class || type == Integer.class || type == float.class || type == Float.class;
        }

        @Override
        public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
            Class<?> type = parameterContext.getParameter().getType();
            if (type == int.class || type == Integer.class) {
                return testCase.numberOfMessages;
            }
            return testCase.percentageWithValues;
        }
    }

    @TestTemplate
    @DisplayName("Binary search handles many gaps across multiple roll cycles")
    public void testBinarySearchWithManyGapsAndManyRollCycles() throws ParseException {
        int searches = runWithTimeParameters(TEST_SECONDLY, 300);
        assertEquals(numberOfMessages + 1, searches, "binary-search: searches (test-secondly)");
    }

    @TestTemplate
    @DisplayName("Binary search handles many gaps within a daily roll cycle")
    public void testBinarySearchWithManyGaps() throws ParseException {
        int searches = runWithTimeParameters(DAILY, 1);
        assertEquals(numberOfMessages + 1, searches, "binary-search: searches (daily)");
    }

    // CPD-OFF - mirrored in TestBinarySearch
    private int runWithTimeParameters(RollCycle rollCycle, long incrementInMillis) {
        final SetTimeProvider stp = new SetTimeProvider();
        stp.currentTimeMillis(0);

        int searches = 0;
        try (SingleChronicleQueue queue = ChronicleQueue.singleBuilder(getTmpDir())
                .rollCycle(rollCycle)
                .timeProvider(stp)
                .build();
             final ExcerptAppender appender = queue.createAppender()) {

            Set<Integer> entriesWithValues = new HashSet<>();
            Random random = new Random();
            for (int i = 0; i < numberOfMessages; i++) {
                try (final DocumentContext dc = appender.writingDocument()) {
                    final MyData myData = new MyData();
                    final boolean putValueAtIndex = random.nextFloat() < percentageWithValues;
                    myData.key = putValueAtIndex ? i : -1;
                    myData.value = "some value where the key=" + i;
                    dc.wire().getValueOut().typedMarshallable(myData);
                    stp.currentTimeMillis(stp.currentTimeMillis() + incrementInMillis);
                    if (putValueAtIndex) {
                        entriesWithValues.add(i);
                    }
                }
            }

            try (final ExcerptTailer tailer = queue.createTailer();
                 final ExcerptTailer binarySearchTailer = queue.createTailer()) {
                for (int j = 0; j < numberOfMessages; j++) {
                    try (DocumentContext ignored = tailer.readingDocument()) {
                        assertNotNull(ignored, "readingDocument returned context at loop index " + j);
                        Wire key = toWire(j);
                        long index = BinarySearch.search(binarySearchTailer, key, GAP_TOLERANT_COMPARATOR);
                        searches++;
                        if (entriesWithValues.contains(j)) {
                            assertEquals(tailer.index(), index, "binary search returns current index for present key at loop index " + j);
                        } else {
                            assertTrue(index < 0, "binary search returns negative for missing key at loop index " + j);
                        }
                        key.bytes().releaseLast();
                    }
                }

                Wire key = toWire(numberOfMessages);
                assertTrue(BinarySearch.search(tailer, key, GAP_TOLERANT_COMPARATOR) < 0,
                        "Binary search should not find non-existent key " + numberOfMessages);
                searches++;
            }
        }
        return searches;
    }

    static class GapTolerantComparator implements Comparator<Wire> {

        @Override
        public int compare(Wire o1, Wire o2) {
            final long readPositionO1 = o1.bytes().readPosition();
            final long readPositionO2 = o2.bytes().readPosition();
            try {
                final MyData myDataO1;
                try (final DocumentContext dc = o1.readingDocument()) {
                    myDataO1 = dc.wire().getValueIn().typedMarshallable();
                }

                final MyData myDataO2;
                try (final DocumentContext dc = o2.readingDocument()) {
                    myDataO2 = dc.wire().getValueIn().typedMarshallable();
                }

                if (myDataO1.key >= 0 && myDataO2.key >= 0) {
                    return Integer.compare(myDataO1.key, myDataO2.key);
                } else {
                    throw NotComparableException.INSTANCE;
                }
            } finally {
                o1.bytes().readPosition(readPositionO1);
                o2.bytes().readPosition(readPositionO2);
            }
        }
    }

    @NotNull
    private Wire toWire(int key) {
        final MyData myData = new MyData();
        myData.key = key;
        myData.value = Integer.toString(key);
        Wire wire = WireType.BINARY.apply(Bytes.allocateElasticOnHeap());
        wire.usePadding(true);

        try (final DocumentContext dc = wire.writingDocument()) {
            dc.wire().getValueOut().typedMarshallable(myData);
        }

        return wire;
    }

    public static class MyData extends SelfDescribingMarshallable {
        private int key;
        private String value;

        @NotNull
        @Override
        public String toString() {
            return "MyData{" +
                    "key=" + key +
                    ", value='" + value + '\'' +
                    '}';
        }
    }
    // CPD-ON
}
