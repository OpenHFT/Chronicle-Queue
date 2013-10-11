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

package net.openhft.chronicle;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * User: peter.lawrey
 * Date: 26/09/13
 * Time: 18:08
 */
public class IndexedChronicleCache {
    private final String basePath;
    private IndexedChronicle chronicle;
    private int chronicleIndex = -1;

    public IndexedChronicleCache(String basePath) {
        this.basePath = basePath;
    }

    public IndexedChronicle acquireChronicle(int index) throws FileNotFoundException {
        if (index == chronicleIndex)
            return chronicle;
        chronicleIndex = index;
        String basePath2 = basePath + "/" + index;
//        System.out.println("Opening " + basePath2);
        return chronicle = new IndexedChronicle(basePath2);
    }

    public void close() throws IOException {
        if (chronicle != null)
            chronicle.close();
    }
}
