package run.chronicle.queue.channel.multi;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.wire.channel.ChronicleGatewayMain;
import run.chronicle.queue.channel.squareService.SquareServiceImpl;
import run.chronicle.queue.channel.squareService.SquareServiceOut;
import run.chronicle.queue.channel.sumservice.SumServiceImpl;
import run.chronicle.queue.channel.sumservice.SumServiceOut;
import run.chronicle.queue.channel.util.ServiceRunner;

import java.io.IOException;

public class MultiServiceMain {
    static final int PORT = 3434;

    public static void main (String[] args) throws IOException {
        final String sumInputQ = "target/sumInputQ";
        final String sumOutputQ = "target/sumOutputQ";
        final String squareInputQ = sumOutputQ;
        final String squareOutputQ = "target/squareOutputQ";

        IOTools.deleteDirWithFiles(sumInputQ, sumOutputQ, squareInputQ, squareOutputQ);

        Runnable sumRunnable = ServiceRunner.serviceRunnable(sumInputQ, sumOutputQ, SumServiceOut.class, SumServiceImpl::new);
        Runnable squareRunnable = ServiceRunner.serviceRunnable(squareInputQ, squareOutputQ, SquareServiceOut.class, SquareServiceImpl::new);
        new Thread(sumRunnable).start();
        new Thread(squareRunnable).start();

        System.setProperty("port", ""+PORT);
        ChronicleGatewayMain.main(args);
    }
}
