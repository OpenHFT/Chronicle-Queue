/*
 * Copyright ${YEAR} Peter Lawrey
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

package net.openhft.chronicle;

import java.io.File;
import java.io.IOException;

/**
 * @author peter.lawrey
 */
public class ChronicleIndex2Main {
    public static void main(String... args) throws IOException, NoSuchFieldException, IllegalAccessException {
        String name = "/mnt/ocz/deleteme";
        new File(name + ".index").delete();
        new File(name + ".index").deleteOnExit();
        long start = System.nanoTime();
        IndexedChronicle chronicle = new IndexedChronicle(name);
        Excerpt excerpt = chronicle.createExcerpt();
        int runs = 250_000_000;
        for (int i = 0; i < runs; i += 2) {
            excerpt.startExcerpt(8);
            excerpt.finish();
            excerpt.startExcerpt(8);
            excerpt.finish();
        }
        chronicle.close();
        long time = System.nanoTime() - start;
        System.out.printf("Updated %,d per second%n", runs * 1000_000_000L / time);
    }
}
