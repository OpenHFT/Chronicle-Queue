/*
 * Copyright 2013 Peter Lawrey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.examples;

import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TObjectLongHashMap;
import net.openhft.chronicle.*;
import net.openhft.chronicle.tools.ChronicleTools;
import net.openhft.lang.io.IOTools;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author peter.lawrey
 */
public class ExampleKeyedExcerptMain {
    private static final String TMP = System.getProperty("java.io.tmpdir");
    @NotNull
    private final Chronicle chronicle;
    @NotNull
    private final Excerpt reader;
    @NotNull
    private final ExcerptTailer tailer;
    @NotNull
    private final ExcerptAppender appender;
    private final TObjectLongMap<String> keyToExcerpt = new TObjectLongHashMap<String>() {
        @Override
        public long getNoEntryValue() {
            return -1;
        }
    };

    public ExampleKeyedExcerptMain(String basePath) throws IOException {
        ChronicleConfig config = ChronicleConfig.DEFAULT.clone();
//        config.indexBlockSize(4*1024);
//        config.dataBlockSize(4*1024);
        chronicle = new IndexedChronicle(basePath, config);
        tailer = chronicle.createTailer();
        appender = chronicle.createAppender();
        reader = chronicle.createExcerpt();
    }

    public static void main(String... ignored) throws IOException {
        String basePath = TMP + "/ExampleKeyedExcerptMain";
        ChronicleTools.deleteOnExit(basePath);
        ExampleKeyedExcerptMain map = new ExampleKeyedExcerptMain(basePath);
        map.load();
        long start = System.nanoTime();
        int keys = 1000000;
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
            Map<String, String> props = new LinkedHashMap<String, String>();
            props.put("a", Integer.toString(i)); // an int.
            props.put("b", "value-" + i); // String
            props.put("c", Double.toString(i / 1000.0)); // a double
            Map<String, String> props2 = map2.getMapFor(Integer.toHexString(i));
            assertEquals("i: " + i, props, props2);
        }
        map2.close();
        long time = System.nanoTime() - start;
        long time2 = System.nanoTime() - start2;
        System.out.printf("Took an average of %,d ns to write and read each entry, an average of %,d ns to lookup%n", time / keys, time2 / keys);
    }

    public void load() {
        while (tailer.nextIndex()) {
            String key = tailer.readUTFΔ();
            keyToExcerpt.put(key, tailer.index());
            tailer.finish();
        }
    }

    public void putMapFor(String key, @NotNull Map<String, String> map) {
        appender.startExcerpt(1024); // a guess
        appender.writeUTFΔ(key);
        appender.writeMap(map);
        appender.finish();
    }

    @NotNull
    public Map<String, String> getMapFor(String key) {
        long value = keyToExcerpt.get(key);
        if (value < 0) return Collections.emptyMap();
        reader.index(value);
        // skip the key
        reader.skip(reader.readStopBit());
        Map<String, String> map = new LinkedHashMap<String, String>();
        reader.readMap(map, String.class, String.class);
        reader.finish();
        return map;
    }

    public void close() {
        IOTools.close(chronicle);
    }
}
