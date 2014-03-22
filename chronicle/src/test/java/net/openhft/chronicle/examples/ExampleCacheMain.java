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
import net.openhft.lang.io.IOTools;
import net.openhft.lang.model.constraints.NotNull;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author ygokirmak
 *         <p></p>
 *         This is just a simple try for a cache implementation
 *         Future Improvements
 *         1- Test multiple writer concurrency and performance
 *         2- Support variable size objects.
 */
public class ExampleCacheMain {
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

    public ExampleCacheMain(String basePath) throws IOException {
        this(basePath, 128 * 1024);
    }

    public ExampleCacheMain(String basePath, int maxObjSize) throws IOException {
        ChronicleConfig config = ChronicleConfig.DEFAULT.clone();
        chronicle = new IndexedChronicle(basePath, config);

        appender = chronicle.createAppender();
        reader = chronicle.createExcerpt();
        _maxObjSize = maxObjSize;

    }

    public static void main(String... ignored) throws IOException {
        String basePath = TMP + "/ExampleCacheMain";
        ChronicleTools.deleteOnExit(basePath);
        ExampleCacheMain map = new ExampleCacheMain(basePath, 200);
        long start = System.nanoTime();
        int keys = 1000000;
        for (int i = 0; i < keys; i++) {
            map.put(i, new Person("name" + i, "surname" + i, i));
        }

        for (int i = 0; i < keys; i++) {
            Person p = (Person) map.get(i);
//			System.out.println(p.get_name() + " " + p.get_surname() + " "
//					+ p.get_age());
        }
        long time = System.nanoTime() - start;
        System.out.printf("Took %.3f secs to add %,d entries%n", time / 1e9, keys);
    }

    public Object get(long key) {
        // Get the excerpt position for the given key from keyIndex map
        long position = keyIndex.get(key);

        // Change reader position
        reader.index(position);

        // Read contents into byte buffer
        Object ret = reader.readObject();

        // validate reading was correct
        reader.finish();

        return ret;
    }

    public void put(long key, Object value) {
        // Start an excerpt with given chunksize
        appender.startExcerpt(_maxObjSize);

        // Write the object bytes
        appender.writeObject(value);

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

    // Took 7.727 secs to add 1,000,000 entries with Serializable
    // Took 2.599  secs to add 1,000,000 entries with Externalizable
    static class Person implements Externalizable {
        private static final long serialVersionUID = 1L;

        private String _name;
        private String _surname;
        private int _age;

        public Person(String name, String surname, int age) {
            _name = name;
            _surname = name;
            _age = age;
        }

        public String get_name() {
            return _name;
        }

        public String get_surname() {
            return _surname;
        }

        public int get_age() {
            return _age;
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeUTF(_name);
            out.writeUTF(_surname);
            out.writeInt(_age);
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            _name = in.readUTF();
            _surname = in.readUTF();
            _age = in.readInt();
        }
    }
}
