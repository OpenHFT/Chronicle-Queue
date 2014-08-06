/*
 * Copyright 2014 Higher Frequency Trading
 * <p/>
 * http://www.higherfrequencytrading.com
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.examples;

import gnu.trove.map.TLongLongMap;
import gnu.trove.map.hash.TLongLongHashMap;
import net.openhft.chronicle.*;
import net.openhft.chronicle.tools.ChronicleTools;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.IOTools;
import net.openhft.lang.io.serialization.BytesMarshallable;
import net.openhft.lang.model.constraints.NotNull;

import java.io.IOException;

/**
 * @author ygokirmak
 *         <p></p>
 *         This is just a simple try for a cache implementation
 *         Future Improvements
 *         1- Test multiple writer concurrency and performance
 *         2- Support variable size objects.
 */
public class ExampleCache2Main {
    private static final String TMP = System.getProperty("java.io.tmpdir");
    @NotNull
    private final Chronicle chronicle;
    @NotNull
    private final Excerpt reader;
    @NotNull
    private final ExcerptAppender appender;
    private final TLongLongMap keyIndex = new TLongLongHashMap() {
        @Override
        public long getNoEntryValue() {
            return -1L;
        }
    };
    private final int _maxObjSize;

    public ExampleCache2Main(String basePath) throws IOException {
        this(basePath, 128 * 1024);
    }

    public ExampleCache2Main(String basePath, int maxObjSize) throws IOException {
        ChronicleConfig config = ChronicleConfig.DEFAULT.clone();
        chronicle = new IndexedChronicle(basePath, config);

        appender = chronicle.createAppender();
        reader = chronicle.createExcerpt();
        _maxObjSize = maxObjSize;

    }

    public static void main(String... ignored) throws IOException {
        String basePath = TMP + "/ExampleCacheMain";
        ChronicleTools.deleteOnExit(basePath);
        ExampleCache2Main map = new ExampleCache2Main(basePath);
        long start = System.nanoTime();
        int keys = 10000000;
        StringBuilder name = new StringBuilder();
        StringBuilder surname = new StringBuilder();
        Person person = new Person(name, surname, 0);
        for (int i = 0; i < keys; i++) {
            name.setLength(0);
            name.append("name");
            name.append(i);

            surname.setLength(0);
            surname.append("surname");
            surname.append(i);

            person.set_age(i);

            map.put(i, person);
        }

        Person person2 = new Person();
        for (int i = 0; i < keys; i++) {
            map.get(i, person);
//			System.out.println(p.get_name() + " " + p.get_surname() + " "
//					+ p.get_age());
        }
        long time = System.nanoTime() - start;
        System.out.printf("Took %.3f secs to add %,d entries%n", time / 1e9, keys);
    }

    public void get(long key, Person person) {
        // Get the excerpt position for the given key from keyIndex map
        long position = keyIndex.get(key);

        // Change reader position
        reader.index(position);

        // Read contents into byte buffer
        person.readMarshallable(reader);

        // validate reading was correct
        reader.finish();
    }

    public void put(long key, Person person) {
        // Start an excerpt with given chunksize
        appender.startExcerpt(_maxObjSize);

        // Write the object bytes
        person.writeMarshallable(appender);

        // pad it for later.
        appender.position(_maxObjSize);

        // Get the position of the excerpt for further access.
        long index = appender.index();

        // TODO Does finish works as "commit" consider transactional
        // consistency between putting key to map and putting object to
        // chronicle
        appender.finish();

        // Put the position of the excerpt with its key to a map.
        keyIndex.put(key, index);
    }

    public void close() {
        IOTools.close(chronicle);
    }

    // Took 5.239 secs to add 10,000,000 entries
    static class Person implements BytesMarshallable {
        private StringBuilder _name;
        private StringBuilder _surname;
        private int _age;

        public Person() {
            this(new StringBuilder(), new StringBuilder(), 0);
        }

        public Person(StringBuilder name, StringBuilder surname, int age) {
            _name = name;
            _surname = surname;
            _age = age;
        }

        public StringBuilder get_name() {
            return _name;
        }

        public StringBuilder get_surname() {
            return _surname;
        }

        public int get_age() {
            return _age;
        }

        public void set_age(int age) {
            _age = age;
        }

        @Override
        public void writeMarshallable(@NotNull Bytes out) {
            out.writeUTFΔ(_name);
            out.writeUTFΔ(_surname);
            out.writeInt(_age);
        }

        @Override
        public void readMarshallable(@NotNull Bytes in) throws IllegalStateException {
            in.readUTFΔ(_name);
            in.readUTFΔ(_surname);
            _age = in.readInt();
        }
    }
}
