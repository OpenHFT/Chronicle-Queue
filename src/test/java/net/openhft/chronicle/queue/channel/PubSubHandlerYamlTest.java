package net.openhft.chronicle.queue.channel;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.wire.utils.YamlTester;
import org.junit.Test;

import static net.openhft.chronicle.queue.channel.PubSubHandlerTest.createTargetDir;
import static org.junit.Assert.assertEquals;


public class PubSubHandlerYamlTest extends QueueTestCommon {
    @Test
    public void yamlTest() {
        String tmpDir = createTargetDir("yamlTest");

        final YamlTester yamlTester = ChannelHandlerYamlTester.runChannelTest(
                tmpDir,
                new PubSubHandler(),
                PubSubHandlerTest.PubSubSays.class,
                PubSubHandlerTest.FromSays.class,
                "queue-pub-sub",
                "tcp://:0");
        IOTools.deleteDirWithFiles(tmpDir);
        assertEquals(yamlTester.expected(), yamlTester.actual());
    }
}