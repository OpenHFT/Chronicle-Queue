package run.chronicle.queue.channel.squareService;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.channel.PublishHandler;
import net.openhft.chronicle.wire.channel.ChannelHandler;
import net.openhft.chronicle.wire.channel.ChronicleChannel;
import net.openhft.chronicle.wire.channel.ChronicleContext;
import net.openhft.chronicle.wire.channel.ChronicleGatewayMain;
import run.chronicle.queue.channel.sumservice.SumServiceImpl;
import run.chronicle.queue.channel.sumservice.SumServiceOut;
import run.chronicle.queue.channel.util.ServiceRunner;

import java.io.IOException;

public class SquareServiceMain {

    static final int PORT = Integer.getInteger("port", 7779);

    public static void main(String[] args) throws IOException {
        final String serviceInputQ = "target/squareInputQ";                                                //  (1)
        final String serviceOutputQ = "target/squareOutputQ";

        IOTools.deleteDirWithFiles(serviceInputQ, serviceOutputQ);

        /*
         * Create an instance of the service and start it, so it is set up for the Gateway.
         */
        Runnable serviceRunnable = ServiceRunner.serviceRunnable(serviceInputQ, serviceOutputQ, SquareServiceOut.class, SquareServiceImpl::new);
        new Thread(serviceRunnable).start();

        System.setProperty("port", "" + PORT);
        ChronicleGatewayMain.main(args);


    }
}
