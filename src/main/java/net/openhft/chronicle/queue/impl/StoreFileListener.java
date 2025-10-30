/*
 * Copyright 2016-2025 chronicle.software
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

import java.io.File;

/**
 * The {@code StoreFileListener} interface provides a mechanism for receiving notifications when a store file is
 * acquired or released. This can be useful in scenarios where certain actions need to be triggered when files are
 * opened or closed, such as managing resources or triggering background processes.
 *
 * <p>The interface provides a default method for determining if the listener is active, and methods for handling
 * file acquisition and release events. It also defines a {@code NO_OP} listener that performs no actions.
 *
 * <p>Listeners implementing this interface may be notified asynchronously when files are acquired or released.
 */
@FunctionalInterface
public interface StoreFileListener {
    @Deprecated(/* use StoreFileListener.noOp() instead, to be removed in x.29 */)
    StoreFileListener NO_OP = new StoreFileListener() {
        @Override
        public boolean isActive() {
            return false;
        }

        @Override
        public void onReleased(int cycle, File file) {
            // no-op
        }
    };

    /**
     * @return a no-op StoreFileListener, which does nothing when files are acquired or released.
     */
    static StoreFileListener noOp() {
        return NO_OP;
    }

    /**
     * @return true if this should be scheduled as a background callback, of false if ignored
     */
    default boolean isActive() {
        return true;
    }

    /**
     * Called when a file is acquired.
     *
     * <p>This method is called asynchronously when a store file is acquired for use, allowing for any
     * necessary handling of the file acquisition event. By default, this method does nothing.
     *
     * @param cycle the cycle associated with the acquired file.
     * @param file the {@link File} object representing the acquired file.
     */
    default void onAcquired(int cycle, File file) {

    }

    /**
     * Called when a file is released.
     *
     * <p>This method is called asynchronously when a store file is released, allowing for any
     * necessary handling of the file release event.
     *
     * @param cycle the cycle associated with the released file.
     * @param file the {@link File} object representing the released file.
     */
    void onReleased(int cycle, File file);
}
