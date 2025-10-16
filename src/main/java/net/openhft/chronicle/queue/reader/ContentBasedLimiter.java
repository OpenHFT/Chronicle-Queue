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

package net.openhft.chronicle.queue.reader;

import net.openhft.chronicle.wire.DocumentContext;

/**
 * Implement this to signal when to halt reading a queue based on the content
 * of a message.
 */
public interface ContentBasedLimiter {

    /**
     * Should the ChronicleReader stop processing messages?
     *
     * @param documentContext The next message to be processed
     * @return true to halt processing before processing the passed message, false otherwise
     */
    boolean shouldHaltReading(DocumentContext documentContext);

    /**
     * Passed to the limiter before processing begins, can be used to provide parameters to it
     * <p>
     * The limiter should make use of {@link Reader#limiterArg()} to convey parameters
     *
     * @param reader The Reader about to be executed
     */
    void configure(Reader reader);
}
