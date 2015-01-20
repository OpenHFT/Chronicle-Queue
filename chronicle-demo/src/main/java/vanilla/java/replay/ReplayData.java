package vanilla.java.replay;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.lang.model.DataValueClasses;

import java.io.IOException;

/**
 * Created by peter on 20/01/15.
 */
public class ReplayData {

    public static void main(String[] args) throws IOException {
        TestData td = DataValueClasses.newInstance(TestData.class);
        String path = "/tmp/test";

        long start = System.nanoTime(), count = 0;
        try (Chronicle chronicle = ChronicleQueueBuilder.indexed(path).build()) {
            ExcerptTailer tailer = chronicle.createTailer();
            while (tailer.nextIndex()) {
                td.readMarshallable(tailer);
                tailer.finish();
                if (td.getAge() != count) {
                    System.out.println(count + ":" + td);
                    break;
                }
                count++;
                if (count % 100000 == 0)
                    System.out.println(td);
            }
        }
        System.out.printf("Took %.3f seconds to read %,d records%n",
                (System.nanoTime() - start) / 1e9, count);
    }
}
