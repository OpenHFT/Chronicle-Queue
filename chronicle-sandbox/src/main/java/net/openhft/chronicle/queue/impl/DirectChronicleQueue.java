/*
 * Copyright 2015 Higher Frequency Trading
 *
 * http://chronicle.software
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

package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.VanillaBytes;
import net.openhft.chronicle.queue.ChronicleQueue;

import java.util.concurrent.atomic.AtomicLong;

public interface DirectChronicleQueue extends ChronicleQueue {

    /**
     * @param buffer the bytes of the document
     * @return the index of the document appended
     */
    long appendDocument(Bytes<?> buffer);

    boolean readDocument(AtomicLong offset, Bytes<?> buffer);

    Bytes<?> bytes();

    /**
     * @return the last index in the chronicle
     * @throws java.lang.IllegalStateException if now data has been written tot he chronicle
     */
    long lastIndex();

    boolean index(long index, VanillaBytes bytes);

    long firstBytes();
}
