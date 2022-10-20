package run.chronicle.queue.channel.sumservice;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.channel.PublishHandler;
import net.openhft.chronicle.wire.channel.ChronicleChannel;
import net.openhft.chronicle.wire.channel.ChronicleContext;

public class SumRequester {

    final static String serviceURL = "tcp://localhost:" + SumServiceMain.PORT;

    public static void main(String[] args) {
        final String serviceInputQ = "target/sumInputQ";

        try (ChronicleContext context = ChronicleContext.newContext(serviceURL)) {
            final ChronicleChannel channel = context.newChannelSupplier(
                new PublishHandler()
                    .publish(serviceInputQ)
            ).get();


            /*
             * Send request through the channel to the service
             */
            double a1 = 2.0, a2 = 4.0;
            Jvm.startup().on(SumRequester.class,">>>>> Sending pair(" + a1 + "," + a2 + ")");

            final SumService adder = channel.methodWriter(SumService.class);
            adder.pair(a1,a2);

        }

    }
}
