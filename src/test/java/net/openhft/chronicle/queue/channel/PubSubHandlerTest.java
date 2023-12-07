package net.openhft.chronicle.queue.channel;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.bytes.PageUtil;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.channel.ChronicleChannel;
import net.openhft.chronicle.wire.channel.ChronicleContext;
import net.openhft.chronicle.wire.channel.ChronicleGatewayMain;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeFalse;

@RunWith(Parameterized.class)
public class PubSubHandlerTest extends QueueTestCommon {

    private final String url;

    public PubSubHandlerTest(String name, String url) {
        this.url = url;
    }

    public static String createTargetDir(String s) {
        String name = OS.getTarget() + "/" + s + "-" + Time.uniqueId();
        if (!PageUtil.isHugePage(name)) {
            // hugepages tests do not run out of target/
            assertTrue(name, name.contains("target/"));
        }
        final File file = new File(name);
        file.mkdirs();
        return name;
    }

    @Parameterized.Parameters(name = "name: {0}, url: {1}")
    public static List<Object[]> combinations() {
        return Arrays.asList(
                new Object[]{"internal", "internal://"},
                new Object[]{"client-only", "tcp://localhost:65451"},
                new Object[]{"server", "tcp://:0"}
        );
    }

    @Override
    @Before
    public void threadDump() {
        super.threadDump();
    }

    @Test
    public void testPubSub() throws IOException {
        final String tmpDir = createTargetDir("testPubSub");
        assumeFalse("PubSubHandler not supported on hugetlbfs", PageUtil.isHugePage(tmpDir));

        try (ChronicleContext context = ChronicleContext.newContext(url).name(tmpDir)) {
            // do we assume a server is running
            if (url.contains("/localhost:")) {
                ChronicleGatewayMain gateway = new ChronicleGatewayMain(this.url);
                gateway.name(tmpDir);
                context.addCloseable(gateway);
                gateway.start();
            }

            ChronicleChannel channel = context.newChannelSupplier(new PubSubHandler()).get();
            PubSubSays pss = channel.methodWriter(PubSubSays.class);

            final String qname = "queue-pub-sub";
            pss.subscribe(new Subscribe().eventType("from").name(qname));

            pss.to(qname).say("Hello");

            Wire wire = Wire.newYamlWireOnHeap();
            final FromSays fromSays = wire.methodWriter(FromSays.class);
            final MethodReader reader = channel.methodReader(fromSays);

            assertFalse(reader.readOne());
            assertTrue(reader.readOne());

            IOTools.deleteDirWithFiles(tmpDir);

            assertEquals("" +
                            "from: " + qname + "\n" +
                            "say: Hello\n" +
                            "...\n",
                    wire.toString());
        } catch (UnsupportedOperationException uos) {
            assumeFalse(url.startsWith("internal"));
        }
    }

    interface PubSubSays extends PubSub {
        Says to(String name);
    }

    interface FromSays {
        Says from(String name);
    }
}