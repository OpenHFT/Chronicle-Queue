package net.openhft.chronicle.queue.channel;

import net.openhft.chronicle.bytes.PageUtil;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.wire.utils.YamlTester;
import org.junit.Test;

import static net.openhft.chronicle.queue.channel.PubSubHandlerTest.createTargetDir;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;

@SuppressWarnings("deprecated")
public class PubSubHandlerYamlTest extends QueueTestCommon {
    @Test
    public void yamlTest() {
        ignoreException("Timeout on ");
        ignoreException("Closed");
        String tmpDir = createTargetDir("yamlTest");
        assumeFalse("PubSubHandler not supported on hugetlbfs", PageUtil.isHugePage(tmpDir));

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
