package net.openhft.chronicle.examples;

import gnu.trove.map.hash.TObjectLongHashMap;
import net.openhft.chronicle.*;
import net.openhft.lang.io.IOTools;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static junit.framework.Assert.assertEquals;

/**
 * @author peter.lawrey
 */
public class ExampleKeyedExcerptMain {
    private static final String TMP = System.getProperty("java.io.tmpdir");

    private final Chronicle chronicle;
    private final ExcerptReader reader;
    private final ExcerptTailer tailer;
    private final ExcerptAppender appender;
    private final TObjectLongHashMap<String> keyToExcerpt = new TObjectLongHashMap<String>() {
        @Override
        public long getNoEntryValue() {
            return -1;
        }
    };

    public ExampleKeyedExcerptMain(String basePath) throws IOException {
        chronicle = new IndexedChronicle(basePath);
        tailer = chronicle.createTailer();
        appender = chronicle.createAppender();
        reader = chronicle.createReader();
    }

    public void load() {
        while (tailer.nextIndex()) {
            String key = tailer.readUTF();
            keyToExcerpt.put(key, tailer.index());
        }
    }

    public void putMapFor(String key, Map<String, String> map) {
        appender.startExcerpt(4096); // a guess
        appender.writeUTF(key);
        appender.writeMap(map);
        appender.finish();
    }

    public Map<String, String> getMapFor(String key) {

        long value = keyToExcerpt.get(key);
        if (value < 0) return Collections.emptyMap();
        reader.index(value);
        // skip the key
        reader.skip(reader.readStopBit());
        return reader.readMap(String.class, String.class);
    }

    public void close() {
        IOTools.close(chronicle);
    }

    public static void main(String... ignored) throws IOException {
        String basePath = TMP + "/ExampleKeyedExcerptMain";
        ChronicleTools.deleteOnExit(basePath);
        ExampleKeyedExcerptMain map = new ExampleKeyedExcerptMain(basePath);
        map.load();
        long start = System.nanoTime();
        int keys = 10000000;
        for (int i = 0; i < keys; i++) {
            Map<String, String> props = new LinkedHashMap<String, String>();
            props.put("a", Integer.toString(i)); // an int.
            props.put("b", "value-" + i); // String
            props.put("c", Double.toString(i / 1000.0)); // a double
            map.putMapFor(Integer.toHexString(i), props);
        }
        map.close();

        ExampleKeyedExcerptMain map2 = new ExampleKeyedExcerptMain(basePath);
        map2.load();
        long start2 = System.nanoTime();
        for (int i = 0; i < keys; i++) {
            Map<String, Object> props = new LinkedHashMap<String, Object>();
            props.put("a", Integer.toString(i)); // an int.
            props.put("b", "value-" + i); // String
            props.put("c", Double.toString(i / 1000.0)); // a double
            Map<String, String> props2 = map2.getMapFor(Integer.toHexString(i));
            assertEquals(props, props2);
        }
        map2.close();
        long time = System.nanoTime() - start;
        long time2 = System.nanoTime() - start2;
        System.out.printf("Took an average of %,d ns to write and read each entry, an average of %,d ns to lookup%n", time / keys, time2 / keys);
    }
}
