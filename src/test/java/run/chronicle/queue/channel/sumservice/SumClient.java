package run.chronicle.queue.channel.sumservice;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.channel.PipeHandler;
import net.openhft.chronicle.wire.channel.ChronicleChannel;
import net.openhft.chronicle.wire.channel.ChronicleContext;

public class SumClient {

    final static String serviceURL = "tcp://localhost:" + SumServiceMain.PORT;

    public static void main(String[] args) {

        final String serviceInputQ = "target/sumInputQ";
        final String serviceOutputQ = "target/sumOutputQ";

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
            double a1 = 2.0, a2 = 4.0;
            Jvm.startup().on(SumClient.class,">>>>> Sending pair(" + a1 + "," + a2 + ")");

            final SumService adder = channel.methodWriter(SumService.class);
            adder.pair(a1,a2);

            /*
             * Collect result and print
             */
            StringBuilder eventType = new StringBuilder();
            double result = channel.readOne(eventType, double.class);
            Jvm.startup().on(SumClient.class, ">>>>> " + eventType + ": " + result);
        }
    }

}
