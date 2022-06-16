/*
 * Copyright 2016-2020 chronicle.software
 *
 * https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.SingleThreadedChecked;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;

/**
 * The ExcerptCommon is common to both ExcerptAppender
 * and ExcerptTailer.
 */
public interface ExcerptCommon<E extends ExcerptCommon<E>> extends Closeable, SingleThreadedChecked {

    /**
     * Returns the source id of the backing ChronicleQueue
     * to which this ExcerptCommon is attached to.
     *
     * @return the source id of the backing ChronicleQueue
     * @see ChronicleQueue#sourceId()
     */
    int sourceId();

    /**
     * Returns the backing ChronicleQueue to which this
     * ExcerptCommon is attached to.
     *
     * @return the backing ChronicleQueue to which this
     * ExcerptCommon is attached to
     */
    @NotNull
    ChronicleQueue queue();

    /**
     * When set to <code>true</code> this Appender or Tailer can be shared between thread provided you ensure they used in a thread safe manner.
     *
     * @param disableThreadSafetyCheck true to turn off the thread safety check
     * @return this.
     */
    @Deprecated(/* to be removed in x.25 */)
    @NotNull
    default E disableThreadSafetyCheck(boolean disableThreadSafetyCheck) {
        singleThreadedCheckDisabled(disableThreadSafetyCheck);
        return (E) this;
    }

    /**
     * @return the current file being worked on or null if not known.
     */
    @Nullable
    default File currentFile() {
        return null;
    }
}
