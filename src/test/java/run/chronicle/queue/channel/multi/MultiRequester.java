package run.chronicle.queue.channel.multi;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.channel.PublishHandler;
import net.openhft.chronicle.wire.channel.ChronicleChannel;
import net.openhft.chronicle.wire.channel.ChronicleContext;
import run.chronicle.queue.channel.sumservice.SumService;

public class MultiRequester {

    final static String serviceURL = "tcp://localhost:" + MultiServiceMain.PORT;

    public static void main(String[] args) {

        final String serviceInputQ = "target/sumInputQ";

        try (ChronicleContext context = ChronicleContext.newContext(serviceURL)) {

            /*
             * Set up channel and PipeHandler to connect to service input and output queues.
             */
            final ChronicleChannel channel = context.newChannelSupplier(
                new PublishHandler()
                    .publish(serviceInputQ)
            ).get();


            /*
             * Send request through the channel to the service
             */
            double a1 = 2, a2 = 3;
            Jvm.startup().on(MultiRequester.class, ">>>>> Sending sum(" + a1 + "," + a2 + ")");

            final SumService sumService = channel.methodWriter(SumService.class);
            sumService.pair(a1, a2);

        }
    }
}
