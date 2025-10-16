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

import net.openhft.chronicle.core.Jvm;

import java.io.File;

public enum StoreFileListeners implements StoreFileListener {
    NO_OP {
        @Override
        public void onReleased(int cycle, File file) {
        }

        @Override
        public boolean isActive() {
            return false;
        }
    },
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
