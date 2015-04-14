/*
 * Copyright 2015 Higher Frequency Trading
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

package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.SingleChronicleQueue;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

/**
 * Created by peter.lawrey on 30/01/15.
 */
public class ChronicleQueueBuilder {
    private String name;
    private long blockSize = 64 << 20;

    public ChronicleQueueBuilder(String name) {
        this.name = name;
    }

    @NotNull
    public ChronicleQueueBuilder blockSize(int blockSize) {
        this.blockSize = blockSize;
        return this;
    }

    @NotNull
    public ChronicleQueue build() throws IOException {
        return new SingleChronicleQueue(name, blockSize);
    }
}
