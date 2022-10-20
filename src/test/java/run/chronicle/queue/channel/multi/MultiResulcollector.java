package run.chronicle.queue.channel.multi;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.channel.PipeHandler;
import net.openhft.chronicle.queue.channel.SubscribeHandler;
import net.openhft.chronicle.wire.channel.ChronicleChannel;
import net.openhft.chronicle.wire.channel.ChronicleContext;
import run.chronicle.queue.channel.sumservice.SumService;

public class MultiResulcollector {

    final static String serviceURL = "tcp://localhost:" + MultiServiceMain.PORT;

    public static void main(String... args) {

        final String serviceOutputQ = "target/squareOutputQ";

        try (ChronicleContext context = ChronicleContext.newContext(serviceURL)) {

            /*
             * Set up channel and PipeHandler to connect to service input and output queues.
             */
            final ChronicleChannel channel = context.newChannelSupplier(
                new SubscribeHandler()
                    .subscribe(serviceOutputQ)
            ).get();

            /*
             * Collect result and print
             */
            StringBuilder eventType = new StringBuilder();
            double result = channel.readOne(eventType, double.class);
            Jvm.startup().on(MultiResulcollector.class, ">>>>> " + eventType + ": " + result);

        }
    }

}
