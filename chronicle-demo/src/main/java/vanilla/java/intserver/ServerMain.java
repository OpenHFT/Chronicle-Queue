package vanilla.java.intserver;

import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.IndexedChronicle;
import net.openhft.chronicle.tools.ChronicleTools;
import vanilla.java.intserver.api.C2SReader;
import vanilla.java.intserver.api.IClient;
import vanilla.java.intserver.api.IServer;
import vanilla.java.intserver.api.S2CWriter;

import java.io.IOException;

public class ServerMain {
    public static void main(String... ignored) throws IOException {
        String tmp = System.getProperty("java.io.tmpdir");

        String c2sPath = tmp + "/demo/c2s";
        ChronicleTools.deleteDirOnExit(c2sPath);
        IndexedChronicle c2s = new IndexedChronicle(c2sPath);
        ExcerptTailer tailer = c2s.createTailer();

        String s2cPath = tmp + "/demo/s2c";
        ChronicleTools.deleteDirOnExit(s2cPath);
        IndexedChronicle s2c = new IndexedChronicle(s2cPath);

        S2CWriter s2CWriter = new S2CWriter(s2c.createAppender());
        ServerHandler server = new ServerHandler(s2CWriter);
        C2SReader reader = new C2SReader(server);

        long prevProcessed = 0, count = 0, readCount = 0;
        //noinspection InfiniteLoopStatement
        do {
            boolean readOne = reader.readOne(tailer);

            if (readOne) {
                // did something
                readCount++;
                count = 0;
            } else if (count++ > 1000000) {
                // do something else like pause.
                long processed = readCount;
                if (prevProcessed != processed) {
                    System.out.printf("Processed %,d requests%n", processed);
                    prevProcessed = processed;
                }
            }
        } while (true);
    }
}

class ServerHandler implements IServer {
    final IClient client;

    ServerHandler(IClient client) {
        this.client = client;
    }

    @Override
    public void command(int request) {
        client.response(request, request + 1, "" + request);
    }
}