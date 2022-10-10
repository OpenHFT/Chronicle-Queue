package run.chronicle.queue.channel.ucservice;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.channel.PipeHandler;
import net.openhft.chronicle.wire.channel.ChannelHandler;
import net.openhft.chronicle.wire.channel.ChronicleChannel;
import net.openhft.chronicle.wire.channel.ChronicleContext;

import java.io.IOException;

public class UCServiceMain {
    static final int PORT = Integer.getInteger("port", 5556);

    public static void main (String[] args) throws IOException {

        final String serviceInput = "target/UCInput";
        final String serviceOutput = "target/UCOutput";

        IOTools.deleteDirWithFiles(serviceInput, serviceOutput);

        try ( ChronicleContext context = ChronicleContext.newContext("tcp://:5556") ) {

            /*
             * Set up a channel with a PipeHandler that publishes from the channel to the service input queue
             * and subscribes to the service output queue and sends back through the channel
             */
            final ChannelHandler handler = new PipeHandler()
                                                .publish(serviceInput)
                                                .subscribe(serviceOutput);
            ChronicleChannel channel = context.newChannelSupplier(handler).get();

            /*
             * Run the service with input and output queues as set up above.
             */
            Runnable handlerRunnable = channel.eventHandlerAsRunnable(
                new UCServiceImpl(
                    channel.methodWriter(UCServiceOut.class)
                )
            );
            new Thread(handlerRunnable).start();

            Jvm.park();
        }
    }
}
