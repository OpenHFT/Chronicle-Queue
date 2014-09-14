package vanilla.java.intserver.api;

import net.openhft.chronicle.ExcerptTailer;

public class C2SReader {
    final IServer server;

    public C2SReader(IServer server) {
        this.server = server;
    }

    public boolean readOne(ExcerptTailer tailer) {
        if (!tailer.nextIndex())
            return false;
        char ch = (char) tailer.readUnsignedByte();
        switch (ch) {
            case C2SWriter.COMMAND: {
                int request = tailer.readInt();
                server.command(request);
                break;
            }
            default:
                System.err.println("Unknown command " + ch);
                break;
        }
        return true;
    }
}
