package net.openhft.chronicle.queue.channel;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.channel.*;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeFalse;

@RunWith(Parameterized.class)
public class PipeHandlerTest extends QueueTestCommon {
    private final boolean buffered;

    public PipeHandlerTest(boolean buffered) {
        this.buffered = buffered;
    }

    @Parameterized.Parameters(name = "buffered: {0}")
    public static List<Object[]> combinations() {
        Object[][] comb = {
                {true},
                {false}
        };
        return Arrays.asList(comb);
    }

    @Before
    public void deleteQueues() {
        IOTools.deleteDirWithFiles("test-q");
    }

    @Test
    @Ignore
    public void internal() {
        String url = "internal://";
        try (ChronicleContext context = ChronicleContext.newContext(url).buffered(buffered)) {
            doTest(context);
        }
    }

    @Test
    public void clientOnly() throws IOException {
        String url = "tcp://localhost:65441";
        IOTools.deleteDirWithFiles("target/client", "target/gw");
        try (ChronicleContext context = ChronicleContext.newContext(url).name("target/client").buffered(buffered)) {
            // do we assume a server is running

            ChronicleGatewayMain gateway = new ChronicleGatewayMain(url);
            gateway.name("target/gw");
            context.addCloseable(gateway);
            gateway.start();

            doTest(context);
        }
    }

    @Test
    public void server() {
        String url = "tcp://:0";
        IOTools.deleteDirWithFiles("target/server");
        try (ChronicleContext context = ChronicleContext.newContext(url).name("target/server").buffered(buffered)) {
            doTest(context);
        }
    }

    @Test
    public void redirectedServer() throws IOException {
        IOTools.deleteDirWithFiles("target/zero", "target/one", "target/client");
        assumeFalse(Jvm.isDebug()); // TODO FIX
        String urlZzz = "tcp://localhost:65329";
        String url0 = "tcp://localhost:65330";
        String url1 = "tcp://localhost:65331";
        try (ChronicleGatewayMain gateway0 = new ChronicleGatewayMain(url0).buffered(buffered)) {
            gateway0.name("target/zero");
            // gateway that will handle the request
            gateway0.start();
            try (ChronicleGatewayMain gateway1 = new ChronicleGatewayMain(url1) {
                @Override
                protected ChannelHeader replaceOutHeader(ChannelHeader channelHeader) {
                    return new RedirectHeader(Arrays.asList(urlZzz, url0));
                }
            }.buffered(buffered)) {
                gateway1.name("target/one");
                // gateway that will handle the redirect request
                gateway1.start();

                try (ChronicleContext context = ChronicleContext.newContext(url1).name("target/client").buffered(buffered)) {
                    doTest(context);
                }
            }
        }
    }

    private void doTest(ChronicleContext context) {
        ChronicleChannel channel = context.newChannelSupplier(new PipeHandler().subscribe("test-q").publish("test-q")).get();
        Says says = channel.methodWriter(Says.class);
        says.say("Hello World");

        StringBuilder eventType = new StringBuilder();
        String text = channel.readOne(eventType, String.class);
        assertEquals("say: Hello World",
                eventType + ": " + text);
        try (DocumentContext dc = channel.readingDocument()) {
            assertFalse(dc.isPresent());
            assertFalse(dc.isMetaData());
        }

        final long now = SystemTimeProvider.CLOCK.currentTimeNanos();
        channel.testMessage(now);
        try (DocumentContext dc = channel.readingDocument()) {
            assertTrue(dc.isPresent());
            assertTrue(dc.isMetaData());
        }
        assertEquals(now, channel.lastTestMessage());
    }
}