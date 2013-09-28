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

import java.io.IOException;
import java.io.Serializable;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleConfig;
import net.openhft.chronicle.Excerpt;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.IndexedChronicle;
import net.openhft.chronicle.tools.ChronicleTools;
import net.openhft.lang.io.IOTools;

import org.jetbrains.annotations.NotNull;

/**
 * @author ygokirmak
 * 
 *         This is just a simple try for a cache implementation 
 *         Future Improvements 
 *         	1- Test multiple writer concurrency and performance 
 *         	2- Support variable size objects.
 *         
 */
public class ExampleCacheMain {
	private static final String TMP = System.getProperty("java.io.tmpdir");
	@NotNull
	private final Chronicle chronicle;
	@NotNull
	private final Excerpt reader;
	@NotNull
	private final ExcerptTailer tailer;
	@NotNull
	private final ExcerptAppender appender;
	private final TObjectLongMap<Long> keyIndex = new TObjectLongHashMap<Long>() {
		@Override
		public long getNoEntryValue() {
			return -1;
		}
	};
	private int _maxObjSize;

	public ExampleCacheMain(String basePath, int maxObjSize) throws IOException {
		ChronicleConfig config = ChronicleConfig.DEFAULT.clone();
		chronicle = new IndexedChronicle(basePath, config);
		tailer = chronicle.createTailer();
		appender = chronicle.createAppender();
		reader = chronicle.createExcerpt();
		_maxObjSize = maxObjSize;

	}

	public static void main(String... ignored) throws IOException {
		String basePath = TMP + "/ExampleCacheMain";
		ChronicleTools.deleteOnExit(basePath);
		ExampleCacheMain map = new ExampleCacheMain(basePath, 1024);
		int keys = 100;
		for (int i = 0; i < keys; i++) {
			map.put(Long.valueOf(i), new Person("name" + i, "surname" + i, i));
		}

		for (int i = 0; i < keys; i++) {
			Person p = (Person) map.get(Long.valueOf(i));
			System.out.println(p.get_name() + " " + p.get_surname() + " "
					+ p.get_age());
		}

	}

	public Object get(Long key) {
		// Get the excerpt position for the given key from keyIndex map
		long position = keyIndex.get(key);

		// Change reader position
		reader.index(position);

		// Read contents into byte buffer
		Object ret = reader.readObject();

		// TODO really understand what reader.finish does
		reader.finish();

		return ret;
	}

	public void put(Long key, Object value) {
		long position = -1;

		// Start an excerpt with given chunksize
		appender.startExcerpt(_maxObjSize);

		// Write the object bytes
		appender.writeObject(value);

		// Get the position of the excerpt for further access.
		position = appender.index();

		// Put the position of the excerpt with its key to a map.
		keyIndex.put(key, position);

		// TODO Does finish works as "commit" consider transactional
		// consistency between putting key to map and putting object to
		// chronicle
		appender.finish();

	}

	public void close() {
		IOTools.close(chronicle);
	}

	static class Person implements Serializable {
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
	}
}
