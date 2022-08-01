package net.openhft.chronicle.queue.channel;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.channel.ChannelHandler;
import net.openhft.chronicle.wire.channel.ChronicleChannel;
import net.openhft.chronicle.wire.channel.ChronicleChannelSupplier;
import net.openhft.chronicle.wire.channel.ChronicleContext;

import java.util.function.Function;

public class Main {
    public static void main(String[] args) {
        final String in = "in";
        final String out = "out";
        IOTools.deleteDirWithFiles(in, out);
        try (ChronicleContext context = ChronicleContext.newContext(args[0])) {
            // start a service
            final ChannelHandler handler0 = new PipeHandler().publish(in).subscribe(out);
            Runnable runs = serviceAsRunnable(context, handler0, EchoingMicroservice::new, Echoed.class);
            new Thread(runs).start();

            // start a client
            final ChannelHandler handler = new PipeHandler().publish(out).subscribe(in);
            final ChronicleChannel channel = context.newChannelSupplier(handler).get();

            // write a message
            final Echoing echoing = channel.methodWriter(Echoing.class);
            echoing.echo(new DummyData());

            // wait for the reply
            try (final DocumentContext dc = channel.readingDocument()) {
                DummyData data = dc.wire().read("echoed").object(DummyData.class);
            }
        }
    }

    public static <I, O> Runnable serviceAsRunnable(ChronicleContext context, ChannelHandler handler, Function<O, I> msFunction, Class<O> tClass) {
        final ChronicleChannelSupplier supplier0 = context.newChannelSupplier(handler);
        final ChronicleChannel channel0 = supplier0.get();
        I microservice = msFunction.apply(channel0.methodWriter(tClass));
        return channel0.eventHandlerAsRunnable(microservice);
    }
}
