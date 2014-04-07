/*
 * Copyright 2014 Peter Lawrey
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
package net.openhft.chronicle.slf4j;

import java.util.Date;

public interface ChronicleLogProcessor {
    /**
     * Text log
     *
     * @param message
     */
    public void process(String message);

    /**
     * Binary log
     *
     * @param timestamp
     * @param level
     * @param threadId
     * @param threadName
     * @param name
     * @param message
     * @param args
     */
    public void process(Date timestamp, int level, long threadId, String threadName, String name, String message, Object... args);
}
