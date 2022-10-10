package run.chronicle.queue.channel.simple;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.wire.channel.ChronicleGatewayMain;

import java.io.IOException;

/*
 * A very simple service that expects the client to set up the PipeHandler for the channel being created
 */
public class ServiceMain {

    static final int PORT = Integer.getInteger("port", 4445);

    public static void main(String[] args) throws IOException {

        System.setProperty("port", "" + PORT);
        ChronicleGatewayMain.main(args);

    }
}
