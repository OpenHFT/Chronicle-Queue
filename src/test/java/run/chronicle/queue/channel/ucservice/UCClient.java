package run.chronicle.queue.channel.ucservice;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.channel.PipeHandler;
import net.openhft.chronicle.wire.channel.ChannelHandler;
import net.openhft.chronicle.wire.channel.ChronicleChannel;
import net.openhft.chronicle.wire.channel.ChronicleContext;

public class UCClient {

    public static void main(String[] args) {
        final String serviceInput = "target/UCInput";
        final String serviceOutput = "target/UCOutput";

        try (ChronicleContext context = ChronicleContext.newContext("tcp://localhost:5556")) {

            String srcMsg = "hello message";

            /*
             * Set up channel and PipeHandler to connect to service input and output queues.
             */
            final ChannelHandler handler = new PipeHandler().publish(serviceInput).subscribe(serviceOutput);
            final ChronicleChannel channel = context.newChannelSupplier(handler).get();

            Jvm.startup().on(UCClient.class, ">>>>> Sending " + srcMsg);

            /*
             * Send test message through the channel to the service
             */
            final UCService uc = channel.methodWriter(UCService.class);
            uc.upperCase(srcMsg);

            /*
             * Read response message from service - display to see transformation to upper case.
             */
            StringBuilder eventType = new StringBuilder();
            String text = channel.readOne(eventType, String.class);

            Jvm.startup().on(UCClient.class, ">>>>> " + eventType + ": " + text);
        }
    }
}
