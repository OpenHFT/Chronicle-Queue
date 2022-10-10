package run.chronicle.queue.channel.simple;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.channel.PipeHandler;
import net.openhft.chronicle.wire.channel.ChronicleChannel;
import net.openhft.chronicle.wire.channel.ChronicleContext;

import java.io.IOException;

/*
 * A client for the basic demo service.
 * Set up a handler that publishes an event to the specified Queue.
 * The handler also subscribes to the same Queue, and so the incoming
 * event is sent straight back through the channel to the client.
 */
public class ChannelClient {

    private static final String URL = System.getProperty("url", "tcp://localhost:" + ServiceMain.PORT);

    public static void main(String[] args) throws IOException {

        final String serviceQ = "target/serviceQ";

        try (ChronicleContext context = ChronicleContext.newContext(URL).name("target/client")) {

            ChronicleChannel channel = context.newChannelSupplier(
                                                        new PipeHandler()
                                                            .subscribe(serviceQ)
                                                            .publish(serviceQ)
                                                )
                                                .get();

            /*
             * Publish event through channel.
             */
            Says says = channel.methodWriter(Says.class);
            says.say("Hello in the Pipe");

            /*
             * Read back event from the channel - should be the same as the one we just sent.
             */
            StringBuilder eventType = new StringBuilder();
            String text = channel.readOne(eventType, String.class);

            Jvm.startup().on(ChannelClient.class, ">>>>> Received " + eventType + ": " + text);
        }
    }
}
