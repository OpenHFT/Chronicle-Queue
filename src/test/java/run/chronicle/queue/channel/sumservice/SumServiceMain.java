package run.chronicle.queue.channel.sumservice;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.channel.PipeHandler;
import net.openhft.chronicle.wire.channel.ChannelHandler;
import net.openhft.chronicle.wire.channel.ChronicleChannel;
import net.openhft.chronicle.wire.channel.ChronicleContext;

public class SumServiceMain {

    static final int PORT = Integer.getInteger("port", 6668);

    public static void main(String[] args) {

        final String serviceInputQ = "target/sumInputQ";
        final String serviceOutputQ = "target/sumOutputQ";

        IOTools.deleteDirWithFiles(serviceInputQ, serviceOutputQ);

        try ( ChronicleContext context = ChronicleContext.newContext("tcp://:"+PORT) ) {

            /*
             * Set up a channel with a PipeHandler that publishes from the channel to the service input queue
             * and subscribes to the service output queue and sends back through the channel
             */
            final ChannelHandler handler = new PipeHandler()
                                                .publish(serviceInputQ)
                                                .subscribe(serviceOutputQ);
            ChronicleChannel channel = context.newChannelSupplier(handler).get();

            /*
             * Run the service with input and output queues as set up above.
             */
            Runnable handlerRunnable = channel.eventHandlerAsRunnable(
                new SumServiceImpl(
                    channel.methodWriter(SumServiceOut.class)
                )
            );
            new Thread(handlerRunnable).start();

            Jvm.park();
        }
    }
}
