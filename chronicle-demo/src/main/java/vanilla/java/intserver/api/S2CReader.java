package vanilla.java.intserver.api;

import net.openhft.chronicle.ExcerptTailer;

public class S2CReader {
    final IClient client;

    public S2CReader(IClient client) {
        this.client = client;
    }

    public boolean readOne(ExcerptTailer tailer) {
        if (!tailer.nextIndex())
            return false;
        char ch = (char) tailer.readUnsignedByte();
        switch (ch) {
            case S2CWriter.RESPONSE: {
                int request = tailer.readInt();
                int response = tailer.readInt();
                int argsLength = tailer.readInt();
                Object[] args = new Object[argsLength];
                for (int i = 0; i < argsLength; i++)
                    args[i] = tailer.readObject();
                client.response(request, response, args);
                break;
            }
            default:
                System.err.println("Unknown command " + ch);
                break;
        }
        return true;
    }
}
