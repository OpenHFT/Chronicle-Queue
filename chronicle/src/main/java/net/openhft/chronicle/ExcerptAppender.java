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

import org.jetbrains.annotations.NotNull;

/**
 * @author peter.lawrey
 */
public interface ExcerptAppender extends ExcerptCommon {
    /**
     * Start an excerpt with the default message capacity of 128K (can be configured)
     * This can waste up to 0.1% of disk space, unless you have sparse file support like Linux, when you will waste far less.
     */
    void startExcerpt();

    /**
     * Ensure there is enough capacity for a new entry of up to the size given.  If there is not enough space left in
     * the chunk of memory mapped file, a padded entry is added and a new entry at the start of a new chunk is
     * commenced.  The capacity can be more than you need as finish() will shrink wrap the entry.  It is onl a waste if
     * you trigger a padded entry when none was required.
     *
     * @param capacity to allow for, but not exceed.
     */
    void startExcerpt(long capacity);

    /**
     * Skip to the last index.
     *
     * @return this Excerpt
     */
    @NotNull
    ExcerptAppender toEnd();

    /**
     * Add a padded entry to keep the index in sync with a master source.
     */
    void addPaddedEntry();

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
}
