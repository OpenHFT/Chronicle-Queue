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

import gnu.trove.map.TLongLongMap;
import gnu.trove.map.hash.TLongLongHashMap;
import net.openhft.chronicle.*;
import net.openhft.chronicle.tools.ChronicleTools;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.IOTools;
import net.openhft.lang.io.serialization.BytesMarshallable;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Random;

/**
 * @author ygokirmak
 */
public class CachePerfMain {
    private static final String TMP = System.getProperty("java.io.tmpdir");
    private static int[] keyArray;
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

    public CachePerfMain(String basePath, int maxObjSize)
            throws IOException {
        ChronicleConfig config = ChronicleConfig.DEFAULT.clone();
        chronicle = new IndexedChronicle(basePath, config);

        appender = chronicle.createAppender();
        reader = chronicle.createExcerpt();
        _maxObjSize = maxObjSize;
    }

    static final int keys = Integer.getInteger("keys", 1000000);

    public static void main(String... ignored) throws IOException {
        String basePath = TMP + "/ExampleCacheMain";
        ChronicleTools.deleteOnExit(basePath);
        CachePerfMain map = new CachePerfMain(basePath, 32);
        long start = System.nanoTime();
        buildkeylist(keys);

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

        long end = System.nanoTime();

        System.out.printf("Took %.3f secs to add %,d entries%n",
                (end - start) / 1e9, keys);

        long duration;
        for (int i = 0; i < 2; i++) {
            duration = randomGet(keys, map);
            System.out.printf(i
                    + "th iter: Took %.3f secs to get seq %,d entries%n",
                    duration / 1e9, keys);
        }

        System.out.println("before shuffle");
        shufflelist();
        System.out.println("after shuffle");

        for (int i = 0; i < 2; i++) {
            duration = randomGet(keys, map);
            System.out.printf(i
                    + "th iter: Took %.3f secs to get random %,d entries%n",
                    duration / 1e9, keys);
        }


    }

    static void shufflelist() {
        Random rnd = new Random();
        int size = keyArray.length;
        for (int i = size; i > 1; i--)
            swap(keyArray, i - 1, rnd.nextInt(i));
    }

    private static void swap(int[] ints, int x, int y) {
        int t = ints[x];
        ints[x] = ints[y];
        ints[y] = t;
    }

    static void buildkeylist(int keycount) {
        keyArray = new int[keycount];
        for (int i = 0; i < keycount; i++) {
            keyArray[i] = i;
        }
    }

    static long randomGet(int keycount, CachePerfMain map) {
        long start = System.nanoTime();
        Person person = new Person();
        for (int i = 0; i < keycount; i++) {
            map.get(keyArray[i], person);
        }
        return System.nanoTime() - start;
    }

    public void get(long key, Person person) {
        // Get the excerpt position for the given key from keyIndex map
// long position = keyIndex.get(key);

        // Change reader position
        reader.index(keyIndex.get(key));
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
        public void readMarshallable(@NotNull Bytes in)
                throws IllegalStateException {
            in.readUTFΔ(_name);
            in.readUTFΔ(_surname);
            _age = in.readInt();
        }
    }
}