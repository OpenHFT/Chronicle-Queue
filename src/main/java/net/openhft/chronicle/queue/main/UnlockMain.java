/*
 * Copyright 2014-2020 chronicle.software
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

package net.openhft.chronicle.queue.main;

import net.openhft.chronicle.queue.internal.main.InternalUnlockMain;

/**
 * UnlockMain is an entry point for unlocking resources or files used by a Chronicle Queue.
 * <p>This utility handles the unlocking of locked resources, such as files, that may be in use by the queue.</p>
 */
public final class UnlockMain {

    /**
     * The main method that triggers the unlocking process.
     * Delegates execution to {@link InternalUnlockMain#main(String[])}.
     *
     * @param args Command-line arguments for unlocking operations
     */
    public static void main(String[] args) {
        InternalUnlockMain.main(args);
    }
}
