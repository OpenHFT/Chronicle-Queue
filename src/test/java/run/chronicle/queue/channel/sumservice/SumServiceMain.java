package run.chronicle.queue.channel.sumservice;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.channel.PipeHandler;
import net.openhft.chronicle.queue.channel.PublishHandler;
import net.openhft.chronicle.queue.channel.SubscribeHandler;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.channel.ChannelHandler;
import net.openhft.chronicle.wire.channel.ChronicleChannel;
import net.openhft.chronicle.wire.channel.ChronicleContext;
import net.openhft.chronicle.wire.channel.ChronicleGatewayMain;
import run.chronicle.queue.channel.util.ServiceRunner;

import java.io.IOException;

public class SumServiceMain {

    static final int PORT = Integer.getInteger("port", 6668);

    public static void main(String[] args) throws IOException {

        final String serviceInputQ = "target/sumInputQ";                                                //  (1)
        final String serviceOutputQ = "target/sumOutputQ";

        IOTools.deleteDirWithFiles(serviceInputQ, serviceOutputQ);

        /*
         * Create an instance of the service and start it, so it is set up for the Gateway.
         */
        Runnable serviceRunnable = ServiceRunner.serviceRunnable(serviceInputQ, serviceOutputQ, SumServiceOut.class, SumServiceImpl::new);
        new Thread(serviceRunnable).start();

        System.setProperty("port", "" + PORT);
        ChronicleGatewayMain.main(args);

    }
}
