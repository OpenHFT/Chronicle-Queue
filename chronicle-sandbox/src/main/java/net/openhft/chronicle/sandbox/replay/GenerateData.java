package net.openhft.chronicle.sandbox.replay;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.lang.model.DataValueClasses;

import java.io.IOException;

/**
 * Created by peter.lawrey on 20/01/15.
 */
public class GenerateData {
    /*
    On an i7-3970X prints.

    Took 1.383 seconds to write 10,000,000 records
     */
    static final long RECORDS = Long.getLong("RECORDS", 10000000);

    public static void main(String[] args) throws IOException {
        TestData td = DataValueClasses.newInstance(TestData.class);
        String path = "/tmp/test";
        StringBuilder name = new StringBuilder();

        long start = System.nanoTime();
        try (Chronicle chronicle = ChronicleQueueBuilder.indexed(path).build()) {
            ExcerptAppender appender = chronicle.createAppender();
            for (long i = 0; i < RECORDS; i++) {
                name.setLength(0);
                td.setName(name.append("Name").append(i));
                td.setAge(i);
                td.setImportance((double) i / RECORDS);
                td.setTimestamp(System.currentTimeMillis());

                appender.startExcerpt();
                td.writeMarshallable(appender);
                appender.finish();
            }
        }
        System.out.printf("Took %.3f seconds to write %,d records%n",
                (System.nanoTime() - start) / 1e9, RECORDS);
    }
}
