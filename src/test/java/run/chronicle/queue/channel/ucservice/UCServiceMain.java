package run.chronicle.queue.channel.ucservice;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.channel.PipeHandler;
import net.openhft.chronicle.wire.channel.ChannelHandler;
import net.openhft.chronicle.wire.channel.ChronicleChannel;
import net.openhft.chronicle.wire.channel.ChronicleContext;
import net.openhft.chronicle.wire.channel.ChronicleGatewayMain;
import run.chronicle.queue.channel.util.ServiceRunner;

import java.io.IOException;

public class UCServiceMain {
    static final int PORT = Integer.getInteger("port", 5556);

    public static void main (String[] args) throws IOException {

        final String serviceInput = "target/UCInput";
        final String serviceOutput = "target/UCOutput";

        IOTools.deleteDirWithFiles(serviceInput, serviceOutput);

        /*
         * Create an instance of the service and start it, so it is set up for the Gateway.
         */
        Runnable serviceRunnable = ServiceRunner.serviceRunnable(serviceInput, serviceOutput, UCServiceOut.class, UCServiceImpl::new);
        new Thread(serviceRunnable).start();

        System.setProperty("port", "" + PORT);
        ChronicleGatewayMain.main(args);

    }
}
