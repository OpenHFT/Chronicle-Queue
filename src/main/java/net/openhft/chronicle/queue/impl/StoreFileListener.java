/*
 * Copyright 2016-2020 chronicle.software
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.queue.impl;

import java.io.File;

@FunctionalInterface
public interface StoreFileListener {
    StoreFileListener NO_OP = StoreFileListeners.NO_OP;

    /**
     * @return true if this should be scheduled as a background callback, of false if ignored
     */
    default boolean isActive() {
        return true;
    }

    /**
     * Notified asynchronously when a file is acquired
     *
     * @param cycle of file
     * @param file  name
     */
    default void onAcquired(int cycle, File file) {

    }

    /**
     * Notified asynchronously when a file is released
     *
     * @param cycle of file
     * @param file  name
     */
    void onReleased(int cycle, File file);
}
