/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
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

package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.core.Jvm;

import java.io.File;

/**
 * The {@code StoreFileListeners} enum provides predefined implementations of the {@link StoreFileListener} interface.
 * These listeners allow for different levels of actions when store files are acquired or released.
 *
 * <p>This enum provides two implementations:</p>
 * <ul>
 *   <li>{@link #NO_OP}: A no-operation listener that performs no actions on file release and is inactive.</li>
 *   <li>{@link #DEBUG}: A listener that logs file release events when debug mode is enabled.</li>
 * </ul>
 */
public enum StoreFileListeners implements StoreFileListener {

    /**
     * A no-operation listener that performs no actions and is inactive.
     */
    NO_OP {
        @Override
        public void onReleased(int cycle, File file) {
            // No-op: does nothing when a file is released
        }

        @Override
        public boolean isActive() {
            return false; // Always inactive
        }
    },

    /**
     * A debug listener that logs file release events when debug mode is enabled.
     *
     * <p>If debugging is enabled, this listener logs a message whenever a store file is released.</p>
     */
    DEBUG {
        @Override
        public void onReleased(int cycle, File file) {
            if (Jvm.isDebugEnabled(getClass()))
                Jvm.debug().on(getClass(), "File released " + file);
        }

        @Override
        public boolean isActive() {
            return Jvm.isDebugEnabled(getClass()); // Active only if debug mode is enabled
        }
    }
}
