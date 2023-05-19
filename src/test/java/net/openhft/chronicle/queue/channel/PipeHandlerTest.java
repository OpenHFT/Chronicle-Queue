package net.openhft.chronicle.queue.channel;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.Mocker;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.channel.*;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.PrintStream;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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

    @Test
    public void filtered() {
        String url = "tcp://:0";
        IOTools.deleteDirWithFiles("target/filtered");

        try (ChronicleContext context = ChronicleContext.newContext(url).name("target/filtered").buffered(buffered);
             ChronicleChannel channel1 = context.newChannelSupplier(createPipeHandler().filter(new SaysFilter(""))).get();
             ChronicleChannel channel2 = context.newChannelSupplier(createPipeHandler().filter(new SaysFilter("2 "))).get();
             ChronicleChannel channel3 = context.newChannelSupplier(createPipeHandler().filter(new SaysFilter("3 "))).get();
        ) {
            Says says1 = channel1.methodWriter(Says.class);
            Says says2 = channel2.methodWriter(Says.class);

            says1.say("1 Hi one");
            says1.say("2 Hi two");
            says1.say("3 Hi three");
            says2.say("1 Bye one");
            says2.say("2 Bye two");
            says2.say("3 Bye three");

            BlockingQueue<String> q = new LinkedBlockingQueue<>();
            MethodReader reader1 = channel1.methodReader(Mocker.queuing(Says.class, "1 - ", q));
            MethodReader reader2 = channel2.methodReader(Mocker.queuing(Says.class, "2 - ", q));
            MethodReader reader3 = channel3.methodReader(Mocker.queuing(Says.class, "3 - ", q));
            readN(reader1, 6);
            readN(reader2, 2);
            readN(reader3, 2);

            assertEquals("" +
                            "1 - say[1 Bye one]\n" +
                            "1 - say[1 Hi one]\n" +
                            "1 - say[2 Bye two]\n" +
                            "1 - say[2 Hi two]\n" +
                            "1 - say[3 Bye three]\n" +
                            "1 - say[3 Hi three]\n" +
                            "2 - say[2 Bye two]\n" +
                            "2 - say[2 Hi two]\n" +
                            "3 - say[3 Bye three]\n" +
                            "3 - say[3 Hi three]",
                    new TreeSet<>(q).stream().collect(Collectors.joining("\n")));
        }
    }

    private static PipeHandler createPipeHandler() {
        return new PipeHandler().subscribe("test-q").publish("test-q").publishSourceId(1);
    }

    private void readN(MethodReader reader, int n) {
        int count = 0;
        while (true) {
            if (reader.readOne())
                count++;
            if (count >= n)
                return;
            Jvm.pause(1);
//            System.out.println("." + n);
        }
    }

    static class SaysFilter extends SelfDescribingMarshallable implements Predicate<Wire> {
        private final String start;

        public SaysFilter(String start) {
            this.start = start;
        }

        @Override
        public boolean test(Wire wire) {
            String said = wire.read("say").text();
            if (said == null)
                return false;
            boolean b = said.startsWith(start);
//            System.out.println("start: " + start + ", said: " + said + ", was: " + b);
            return b;
        }
    }
}