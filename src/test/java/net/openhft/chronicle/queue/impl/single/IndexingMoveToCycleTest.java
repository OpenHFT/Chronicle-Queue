/*
 * Copyright 2016-2025 chronicle.software
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
package net.openhft.chronicle.queue.impl.single;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class IndexingMoveToCycleTest extends IndexingTestCommon {

    /**
     * The behaviour of moveToCycle is undefined for invalid cycles. Moving to a non-existent cycle puts the tailer into
     * an inconsistent internal state.
     */
    @Test
    void noDataMoveToNegativeCycle() {
        assertFalse(tailer.moveToCycle(-1));
        assertEquals(-2147483648, tailer.cycle());
    }

    @Test
    void noDataMoveToNonExistentCycle() {
        assertFalse(tailer.moveToCycle(1));
        assertEquals(-2147483648, tailer.cycle());
    }

    @Test
    void someDataMoveToNonExistentCycle() {
        appender.writeText("test");
        assertFalse(tailer.moveToCycle(1));
        assertEquals(-2147483648, tailer.cycle());
        assertEquals("test", tailer.readText());
    }
}
