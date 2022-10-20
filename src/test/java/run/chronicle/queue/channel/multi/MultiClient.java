package run.chronicle.queue.channel.multi;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.channel.PipeHandler;
import net.openhft.chronicle.wire.channel.ChronicleChannel;
import net.openhft.chronicle.wire.channel.ChronicleContext;
import run.chronicle.queue.channel.sumservice.SumService;

public class MultiClient {


    final static String serviceURL = "tcp://localhost:" + MultiServiceMain.PORT;

    public static void main(String[] args) {

        final String serviceInputQ = "target/sumInputQ";
        final String serviceOutputQ = "target/squareOutputQ";

        try (ChronicleContext context = ChronicleContext.newContext(serviceURL)) {

            /*
             * Set up channel and PipeHandler to connect to service input and output queues.
             */
            final ChronicleChannel channel = context.newChannelSupplier(
                new PipeHandler()
                    .publish(serviceInputQ)
                    .subscribe(serviceOutputQ)
            ).get();


            /*
             * Send request through the channel to the service
             */
            double a1 = 2, a2 = 3;
            Jvm.startup().on(MultiClient.class, ">>>>> Sending pair(" + a1 + "," + a2 + ")");

            final SumService sumService = channel.methodWriter(SumService.class);
            sumService.pair(a1, a2);

            /*
             * Collect result and print
             */
            StringBuilder eventType = new StringBuilder();
            double result = channel.readOne(eventType, double.class);
            Jvm.startup().on(MultiClient.class, ">>>>> " + eventType + ": " + result);
        }

    }
}
