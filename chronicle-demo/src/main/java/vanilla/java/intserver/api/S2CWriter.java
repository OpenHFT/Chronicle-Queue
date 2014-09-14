package vanilla.java.intserver.api;

import net.openhft.chronicle.ExcerptAppender;

public class S2CWriter implements IClient {
    static final char RESPONSE = 'r';
    final ExcerptAppender excerpt;

    public S2CWriter(ExcerptAppender excerpt) {
        this.excerpt = excerpt;
    }

    @Override
    public void response(int request, int response, Object... args) {
        excerpt.startExcerpt();
        excerpt.writeByte(RESPONSE);
        excerpt.writeInt(request);
        excerpt.writeInt(response);
        excerpt.writeInt(args.length);
        for (Object arg : args) {
            excerpt.writeObject(response);
        }
        excerpt.finish();
    }
}
