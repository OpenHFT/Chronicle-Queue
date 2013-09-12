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

import net.openhft.lang.io.Bytes;

/**
 * @author peter.lawrey
 */
public interface ExcerptCommon extends Bytes {
    /**
     * Randomly select an Excerpt.
     *
     * @param l index to look up
     * @return true if this is a valid entries and not padding.
     */
    boolean index(long l);

    /**
     * @return true if the last index(long) looked up as padding.
     */
    boolean wasPadding();

    /**
     * @return this appender is pointing.
     */
    long index();

    /**
     * @return the index last written to including padded entries.
     */
    long lastWrittenIndex();

    /**
     * This is an upper bound for the number of entires available.  This includes padded entries.
     *
     * @return lastWrittenIndex() + 1
     */
    long size();

    /**
     * Wind to the end.
     *
     * @return this Excerpt
     */
    ExcerptCommon toEnd();

    /**
     * @return the chronicle associated with this Excerpt
     */
    Chronicle chronicle();

    /**
     * Finish reading or writing.  This checks there was not a buffer overflow and shrink wraps new entries and adds
     * them to the index
     */
    void finish();
}
