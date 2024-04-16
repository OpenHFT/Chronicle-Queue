package net.openhft.chronicle.queue.channel;

import net.openhft.chronicle.bytes.PageUtil;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.wire.utils.YamlTester;
import org.junit.Test;

import static net.openhft.chronicle.queue.channel.PubSubHandlerTest.createTargetDir;
import static org.junit.Assume.assumeFalse;

@SuppressWarnings("deprecated")
public class PipeHandlerYamlTest extends QueueTestCommon {
    @Test
    public void yamlTest() {
        ignoreException("Timeout on ");
        String tmpDir = createTargetDir("yamlTest");
        assumeFalse("PipeHandler not supported on hugetlbfs", PageUtil.isHugePage(tmpDir));

        final String qname = "test-q-yaml";
        final PipeHandler handler = new PipeHandler().subscribe(qname).publish(qname);

        final YamlTester yamlTester = ChannelHandlerYamlTester.runChannelTest(tmpDir, handler, Says.class, Says.class, "queue-says", "tcp://:0");
        IOTools.deleteDirWithFiles(tmpDir);
        TestUtil.allowCommentsOutOfOrder(yamlTester);
    }
}
