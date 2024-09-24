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

package net.openhft.chronicle.queue.reader;

import net.openhft.chronicle.wire.DocumentContext;

/**
 * Interface for signaling when to halt reading from a queue based on the content of a message.
 * <p>This can be used to limit processing within the {@link ChronicleReader} based on specific conditions found in the messages.</p>
 */
public interface ContentBasedLimiter {

    /**
     * Determines whether the {@link ChronicleReader} should stop processing further messages.
     * <p>This method examines the content of the next message and decides if reading should halt before processing it.</p>
     *
     * @param documentContext The document context representing the next message to be processed
     * @return {@code true} to halt processing, {@code false} to continue processing the message
     */
    boolean shouldHaltReading(DocumentContext documentContext);

    /**
     * Configures the limiter with parameters before the reader begins processing.
     * <p>This method allows the limiter to be customized using arguments provided via {@link Reader#limiterArg()}.</p>
     *
     * @param reader The reader that is about to be executed, providing context and parameters for the limiter
     */
    void configure(Reader reader);
}
