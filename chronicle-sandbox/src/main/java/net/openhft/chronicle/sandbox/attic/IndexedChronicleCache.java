/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.sandbox.attic;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;

import java.io.IOException;

/**
 * User: peter.lawrey
 * Date: 26/09/13
 * Time: 18:08
 */
public class IndexedChronicleCache {
    private final String basePath;
    private Chronicle chronicle;
    private int chronicleIndex = -1;

    public IndexedChronicleCache(String basePath) {
        this.basePath = basePath;
    }

    public Chronicle acquireChronicle(int index) throws IOException {
        if (index == chronicleIndex)
            return chronicle;
        chronicleIndex = index;
        String basePath2 = basePath + "/" + index;
//        System.out.println("Opening " + basePath2);
        return chronicle = ChronicleQueueBuilder.indexed(basePath2).build();
    }

    public void close() throws IOException {
        if (chronicle != null)
            chronicle.close();
    }
}
