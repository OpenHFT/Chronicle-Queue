package net.openhft.chronicle.queue.channel;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.ClosedIORuntimeException;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.YamlWire;
import net.openhft.chronicle.wire.channel.ChannelHandler;
import net.openhft.chronicle.wire.channel.ChronicleChannel;
import net.openhft.chronicle.wire.channel.ChronicleContext;
import net.openhft.chronicle.wire.channel.TesterControl;
import net.openhft.chronicle.wire.utils.YamlTester;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class ChannelHandlerYamlTester implements YamlTester {

    static final TesterControl testerControl = Jvm::pause;
    private static final AtomicInteger ai = new AtomicInteger();
    private final String expected;
    private final String actual;

    private ChannelHandlerYamlTester(String expected, String actual) {

        this.expected = expected;
        this.actual = actual;
    }

    public static YamlTester runChannelTest(String name, ChannelHandler handler, Class<?> inClass, Class<?> outClass, String path, String url) {
        // ensure it's loaded and compiled
        Wire.newYamlWireOnHeap().methodWriter(outClass).getClass();
        return runChannelTest(name, handler, inClass, w -> w.methodWriter(outClass), path, url);
    }

    public static YamlTester runChannelTest(String name, ChannelHandler handler, Class<?> inClass, Function<WireOut, ?> outFunction, String path, String url) {
        try (ChronicleContext context = ChronicleContext.newContext(url).name(name);
             ChronicleChannel channel = context.newChannelSupplier(handler).get()) {
            final Object in = channel.methodWriter(inClass);
            // setup.yaml
            try {
                final byte[] byteArray = IOTools.readFile(inClass, path + "/setup.yaml");
                final Bytes<?> bytes = Bytes.wrapForRead(byteArray);
                Wire wire0 = new YamlWire(bytes).useTextDocuments();
                MethodReader reader0 = wire0.methodReaderBuilder()
                        .warnMissing(true)
                        .build(in, testerControl);
                long now = 1;
                while (reader0.readOne()) {
                    now = SystemTimeProvider.CLOCK.currentTimeNanos();
                    channel.testMessage(now);

                    while (!channel.isClosed()) {
                        try (DocumentContext dc = channel.readingDocument()) {
                            if (!dc.isPresent())
                                break;
                            if (channel.lastTestMessage() >= now)
                                break;
                        }
                    }
                }

                final long lastTestResponse = channel.lastTestMessage();
                if (lastTestResponse != now) {
                    if (lastTestResponse == 0)
                        return new ChannelHandlerYamlTester("lastTestResponse: " + now, "no testResponse");
                    return new ChannelHandlerYamlTester("lastTestResponse: " + now, "lastTestResponse" + lastTestResponse);
                }
            } catch (FileNotFoundException ignored) {
            } catch (IOException ioe) {
                throw new IORuntimeException(ioe);
            }
            // in.yaml
            String expected;
            try {
                final byte[] expectedBytes = IOTools.readFile(inClass, path + "/out.yaml");
                expected = new String(expectedBytes, StandardCharsets.ISO_8859_1);
                if (OS.isWindows())
                    expected = expected.replace("\r\n", "\n");
                final Bytes<?> bytes = Bytes.wrapForRead(IOTools.readFile(inClass, path + "/in.yaml"));
                Wire wire0 = new YamlWire(bytes).useTextDocuments();
                MethodReader reader0 = wire0.methodReaderBuilder()
                        .warnMissing(true)
                        .build(in, testerControl);
                Wire wire = Wire.newYamlWireOnHeap();
                wire0.commentListener(wire::writeComment);
                final Object writer = outFunction.apply(wire);
                final MethodReader reader1 = channel.methodReader(writer);
                long now = 1;
                AtomicLong activeTime = new AtomicLong(System.currentTimeMillis());
                final Thread thread = new Thread(() -> {
                    long timeout = Jvm.isDebug() ? 60_000 : 500;
                    for (int i = 0; i < 500; i++) {
                        Jvm.pause(1);
                        if (activeTime.get() + timeout < System.currentTimeMillis())
                            break;
                    }
                    if (channel.isClosed())
                        return;
                    Jvm.warn().on(ChannelHandlerYamlTester.class, "Timeout on " + channel + ", closing");
                    channel.close();
                });
                thread.start();
                try {
                    copyComments(wire0, wire);
                    wire.bytes().append("---\n");
                    while (reader0.readOne()) {
                        activeTime.set(System.currentTimeMillis());
                        now = SystemTimeProvider.CLOCK.currentTimeNanos();
                        channel.testMessage(now);

                        int count = 0;
                        while (channel.lastTestMessage() < now) {
                            if (reader1.readOne()) {
                                count = 0;
                            } else {
                                count++;
                                if (count > 1)
                                    throw new AssertionError();
                            }
                        }
                        if (wire0.bytes().startsWith(Bytes.from("...\n")))
                            wire0.bytes().readSkip(4);
                        copyComments(wire0, wire);
                        wire.bytes().append("---\n");
                    }

                    while (wire.bytes().readRemaining() < expectedBytes.length) {
                        activeTime.set(System.currentTimeMillis());
                        reader1.readOne();
                    }
                    final long lastTestResponse = channel.lastTestMessage();
                    if (lastTestResponse != now) {
                        if (lastTestResponse == 0)
                            return new ChannelHandlerYamlTester("lastTestResponse: " + now, "no testResponse");
                        return new ChannelHandlerYamlTester("lastTestResponse: " + now, "lastTestResponse: " + lastTestResponse);
                    }

                } catch (ClosedIORuntimeException | IllegalStateException ignored) {

                } catch (Exception e) {
                    final StringWriter out = new StringWriter();
                    PrintWriter pw = new PrintWriter(out);
                    e.printStackTrace(pw);
                    wire.bytes().append(out.toString());
                } finally {
                    thread.interrupt();
                }
                return new ChannelHandlerYamlTester(expected, wire.toString());

            } catch (IOException ioe) {
                throw new IORuntimeException(ioe);
            }
        }
    }

    private static void copyComments(Wire from, Wire to) {
        final Bytes<?> fromBytes = from.bytes();
        while (fromBytes.readRemaining() > 0) {
            while (Character.isWhitespace(fromBytes.peekUnsignedByte()))
                fromBytes.readSkip(1);
            if (fromBytes.peekUnsignedByte() != '#')
                return;

            fromBytes.readSkip(1);
            to.bytes().writeUnsignedByte('#');
            while (fromBytes.readRemaining() > 0) {
                int ch = fromBytes.readUnsignedByte();
                if (ch < ' ')
                    ch = '\n';
                to.bytes().writeUnsignedByte(ch);
                if (ch < ' ')
                    break;
            }
        }
    }

    @Override
    public String expected() {
        return expected;
    }

    @Override
    public String actual() {
        return actual;
    }
}
