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
                                                            .publish(serviceOutputQ)
                                                            .subscribe(serviceInputQ)
                                                    ).get();


            /*
             * Send request through the channel to the service
             */
            double a1 = 2.5, a2 = 4.2;
            Jvm.startup().on(SumClient.class,">>>>> Sending sum(" + a1 + "," + a2 + ")");

            final SumService adder = channel.methodWriter(SumService.class);
            adder.sum(a1,a2);

            /*
             * Collect result and print
             */
            StringBuilder eventType = new StringBuilder();
            String result = channel.readOne(eventType, String.class);
            Jvm.startup().on(SumClient.class, ">>>>> " + eventType + ": " + result);
        }
    }

}
