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

package net.openhft.chronicle.queue.impl.table;

/**
 * Enum representing the action to take when lock acquisition times out in a {@link TableStore}.
 * It defines different strategies for handling lock acquisition failures.
 */
public enum UnlockMode {

    /**
     * Always force unlock and re-acquire the lock, regardless of the state of the locking process.
     */
    ALWAYS,

    /**
     * Never force unlock. Instead, an exception will be thrown if lock acquisition times out.
     */
    NEVER,

    /**
     * Force unlock and re-acquire the lock only if the process holding the lock is no longer alive.
     * If the locking process is still running, an exception is thrown.
     */
    LOCKING_PROCESS_DEAD
}
