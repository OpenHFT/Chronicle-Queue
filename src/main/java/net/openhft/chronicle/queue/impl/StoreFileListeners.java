/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.core.Jvm;

import java.io.File;

/**
 * The {@code StoreFileListeners} enum provides predefined implementations of the {@link StoreFileListener} interface.
 * These listeners allow for different levels of actions when store files are acquired or released.
 *
 * <p>This enum provides two implementations:
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
        }

        @Override
        public boolean isActive() {
            return false;
        }
    },

    /**
     * A debug listener that logs file release events when debug mode is enabled.
     *
     * <p>If debugging is enabled, this listener logs a message whenever a store file is released.
     */
    DEBUG {
        @Override
        public void onReleased(int cycle, File file) {
            if (Jvm.isDebugEnabled(getClass()))
                Jvm.debug().on(getClass(), "File released " + file);
        }

        @Override
        public boolean isActive() {
            return Jvm.isDebugEnabled(getClass());
        }
    }
}
