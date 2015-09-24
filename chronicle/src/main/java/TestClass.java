import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;

import java.io.IOException;

/**
 * Created by FatihDonmez on 28/03/15.
 */
public class TestClass {

    public static void main(String[] args) throws IOException {

        Chronicle c = ChronicleQueueBuilder.vanilla("/chronicle/fatih").
                cycleFormat("yyyyMMdd-HHmm").cycleLength(60*60000).build();

        ExcerptAppender appender = c.createAppender();

        appender.startExcerpt(4);
        appender.writeInt(12);
        appender.finish();



      //  VanillaChronicle.VanillaTailer tailer = (VanillaChronicle.VanillaTailer) c.createTailer();

      //  System.out.println(tailer);
    }
}
