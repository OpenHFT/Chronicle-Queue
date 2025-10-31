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

import org.junit.Test;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class NoOpConditionTest {

    @Test
    public void noOpMethodsReturnImmediately() throws Exception {
        NoOpCondition c = NoOpCondition.INSTANCE;
        c.await();
        c.awaitUninterruptibly();
        assertEquals(123L, c.awaitNanos(123L));
        assertTrue(c.await(1, TimeUnit.MILLISECONDS));
        assertTrue(c.awaitUntil(new Date(System.currentTimeMillis())));
        c.signal();
        c.signalAll();
    }
}

