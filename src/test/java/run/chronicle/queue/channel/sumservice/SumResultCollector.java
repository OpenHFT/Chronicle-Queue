package run.chronicle.queue.channel.sumservice;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.channel.PublishHandler;
import net.openhft.chronicle.queue.channel.SubscribeHandler;
import net.openhft.chronicle.wire.channel.ChronicleChannel;
import net.openhft.chronicle.wire.channel.ChronicleContext;

public class SumResultCollector {

    final static String serviceURL = "tcp://localhost:" + SumServiceMain.PORT;

    public static void main(String[] args) {
        final String serviceOutputQ = "target/sumOutputQ";

        try (ChronicleContext context = ChronicleContext.newContext(serviceURL)) {
            final ChronicleChannel channel = context.newChannelSupplier(
                new SubscribeHandler()
                    .subscribe(serviceOutputQ)
            ).get();


            StringBuilder eventType = new StringBuilder();
            double result = channel.readOne(eventType, double.class);
            Jvm.startup().on(SumResultCollector.class, ">>>>> " + eventType + ": " + result);

        }

    }

}
