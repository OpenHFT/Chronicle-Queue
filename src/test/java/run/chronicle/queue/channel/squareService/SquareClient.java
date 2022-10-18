package run.chronicle.queue.channel.squareService;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.channel.PipeHandler;
import net.openhft.chronicle.wire.channel.ChronicleChannel;
import net.openhft.chronicle.wire.channel.ChronicleContext;
import run.chronicle.queue.channel.sumservice.SumClient;
import run.chronicle.queue.channel.sumservice.SumService;
import run.chronicle.queue.channel.sumservice.SumServiceMain;

public class SquareClient {

    final static String serviceURL = "tcp://localhost:" + SquareServiceMain.PORT;

    public static void main(String[] args) {

        final String serviceInputQ = "target/squareInputQ";
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
            double a1 = 2;
            Jvm.startup().on(SumClient.class, ">>>>> Sending square(" + a1 + ")");

            final SquareService squarer = channel.methodWriter(SquareService.class);
            squarer.toSquare(a1);

            /*
             * Collect result and print
             */
            StringBuilder eventType = new StringBuilder();
            double result = channel.readOne(eventType, double.class);
            Jvm.startup().on(SumClient.class, ">>>>> " + eventType + ": " + result);
        }

    }
}
