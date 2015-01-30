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

import net.openhft.chronicle.wire.WireIn;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.model.constraints.NotNull;

/**
 * The component that facilitates sequentially reading data from a {@link Chronicle}.
 *
 * @author peter.lawrey
 */
public interface ExcerptTailer extends ExcerptCommon {
    /**
     * @return the wire associated with this tailer.
     */
    public WireIn wire();

    /**
     * @return the underlying Bytes
     */
    public Bytes bytes();
    
    /**
     * Randomly select an Excerpt.
     *
     * @param l index to look up
     * @return true if this is a valid entries and not padding.
     */
    boolean index(long l);

    /**
     * Wind to the next entry, no matter how many padded index entries you need to skip. If there is padding, the
     * index() can change even though this method might return false
     *
     * @return Is there a valid entry to be read.
     */
    boolean nextIndex();

    /**
     * Replay from the start.
     *
     * @return this Excerpt
     */
    @NotNull
    ExcerptTailer toStart();

    /**
     * Wind to the end.
     *
     * @return this Excerpt
     */
    @NotNull
    ExcerptTailer toEnd();
}
