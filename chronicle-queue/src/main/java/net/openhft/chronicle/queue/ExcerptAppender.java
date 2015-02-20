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

package net.openhft.chronicle.queue;


import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.Nullable;

import java.util.function.Consumer;

/**
 * The component that facilitates sequentially writing data to a {@link ChronicleQueue}.
 *
 * @author peter.lawrey
 */
public interface ExcerptAppender extends ExcerptCommon {
    /**
     * @return the underlying Wire.
     */
    @Nullable
    public WireOut wire();

    /**
     * @param writer to write one excerpt.
     */
    public void writeDocument(Consumer<WireOut> writer);

    /**
     * The default value is ChronicleConfig.synchronousMode()
     *
     * @return will the next write be synchronous
     */
    boolean nextSynchronous();

    /**
     * @param nextSynchronous make the next write synchronous or not.
     */
    void nextSynchronous(boolean nextSynchronous);

    /**
     * @return the index last written to including padded entries.
     */
    long lastWrittenIndex();
}
