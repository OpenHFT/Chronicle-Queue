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

package net.openhft.chronicle.queue.impl.table;

/**
 * Determines the action to take if lock acquisition times out
 */
public enum UnlockMode {
    /**
     * force unlock and re-acquire
     */
    ALWAYS,
    /**
     * throw exception
     */
    NEVER,
    /**
     * force unlock and re-acquire only if the locking process is dead, otherwise throw exception
     */
    LOCKING_PROCESS_DEAD
}
