package run.chronicle.queue.channel.util;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

import java.util.function.Function;

public class ServiceRunner<T> {

    /*
     * Create a Runnable that will run the given service.
     * The service is run in a way that is siliar to that provided by Chronicle Services, but is much simpler and
     * does not include any of the Enterprise Features of Chronicle Services.
     * Arguments are
     *   - pathnames of input and output queues (only one of each is supported here)
     *   - class of the output interface for the service to facilitate creation of a MethodWriter
     *   - function that creates an instance of the service using a parameter supporting the injection
     *     of a MethodWriter for output
     * By creating a Runnable we allow the service to be run "in thread" or in a separate thread context.
     */
    public static <S, T> Runnable serviceRunnable(String inputQueue, String outputQueue, Class<T> outClass, Function<T, S> serviceProvider) {
        return  () -> {
            /*
             * Set up the input and output queues.
             */
            try (ChronicleQueue in = SingleChronicleQueueBuilder.binary(inputQueue).build();
                 ChronicleQueue out = SingleChronicleQueueBuilder.binary(outputQueue).build()) {

                /*
                 * Create a Chronicle Wire MethodWriter that will serialise output event(s) to the
                 * output queue.
                 */
                T outputWriter = out.acquireAppender().methodWriter(outClass);

                /*
                 * Use the provided method (normally constructor_) to create an instance of the service, injecting the
                 * MethodWriter created above for output.
                 */
                S service = serviceProvider.apply(outputWriter);

                /*
                 * Create a method reader to read events from the input queue, then dispatch these to the appropriate
                 * methods in the service implementation
                 */
                MethodReader inputReader = in.createTailer().toEnd().methodReader(service);
                while (!Thread.currentThread().isInterrupted()) {
                    if (!inputReader.readOne())
                        Jvm.pause(10);
                }
            }
        };
    }
}
