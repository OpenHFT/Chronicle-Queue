package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.wire.DocumentContext;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

/**
 * Created by Rob Austin
 */
public class BinaryWireHeaderTest {

    @Test
    public void testHeader() {

        System.setProperty("includeQueueHeaderField-lastIndexReplicated-and-sourceId", "true");

        File testHeader = getTmpDir
                ("testHeader");
        try (final SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(testHeader)
                .build()) {
            try (DocumentContext documentContext = queue.acquireAppender().writingDocument()) {
                documentContext.wire().write("somekey").text("somevalue");
            }
            Assert.assertTrue(queue.dump().startsWith("--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: [\n" +
                    "    262736,\n" +
                    "    1128442527481856\n" +
                    "  ],\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: !short 16384,\n" +
                    "    indexSpacing: 16,\n" +
                    "    index2Index: 520,\n" +
                    "    lastIndex: 16\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  lastIndexReplicated: -1,\n" +
                    "  sourceId: 0,\n" +
                    "  isSyncQueueConnectedViaTcpIp: false,\n" +
                    "  timeLastMessageReceivedViaTcpIp: 0\n" +
                    "}\n"));

        }

        try (final SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(testHeader)
                .build()) {
            try (DocumentContext documentContext = queue.createTailer().readingDocument()) {
                Assert.assertEquals("somevalue", documentContext.wire().read("somekey").text());
            }

        }
    }

    @NotNull
    protected File getTmpDir(String methodName) {
        return DirectoryUtils.tempDir(methodName != null ?
                methodName.replaceAll("[\\[\\]\\s]+", "_") : "NULL-" + UUID.randomUUID());
    }

}
