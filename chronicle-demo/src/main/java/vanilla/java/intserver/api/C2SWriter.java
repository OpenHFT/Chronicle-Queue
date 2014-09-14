package vanilla.java.intserver.api;

import net.openhft.chronicle.ExcerptAppender;

public class C2SWriter implements IServer {
    public static final char COMMAND = 'c';
    final ExcerptAppender excerpt;

    public C2SWriter(ExcerptAppender excerpt) {
        this.excerpt = excerpt;
    }

    @Override
    public void command(int request) {
        excerpt.startExcerpt();
        excerpt.writeByte(COMMAND);
        excerpt.writeInt(request);
        excerpt.finish();
    }
}
