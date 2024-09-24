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

/**
 * Represents a message consumer in a chain-of-responsibility pattern.
 * <p>Message consumers can filter or transform input messages as they are passed through the chain.
 * The final destination of the message is the sink, which is the last consumer in the pipeline.</p>
 */
public interface MessageConsumer {

    /**
     * Consumes the given message, performing any filtering or transformation as necessary.
     *
     * @param index   The index of the message within the queue
     * @param message The message content as a string
     * @return {@code true} if the message was passed through to the sink, {@code false} if it was filtered out
     */
    boolean consume(long index, String message);
}
