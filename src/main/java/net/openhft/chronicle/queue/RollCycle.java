/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.time.TimeProvider;

public interface RollCycle {

    String format();

    int length();

    int defaultIndexCount();

    int defaultIndexSpacing();

    /**
     * @param epoch and EPOCH offset, to all the user to define their own epoch
     * @return the cycle
     */
    int current(TimeProvider time, long epoch);

    long toIndex(int cycle, long sequenceNumber);

    long toSequenceNumber(long index);

    int toCycle(long index);
}
