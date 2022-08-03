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
 * Message consumers make a chain-of-responsibility pattern, they
 * can filter or transform the input. The sink will be the final
 * consumer in the pipeline.
 */
public interface MessageConsumer {

    /**
     * Consume the message
     *
     * @param index   the index of the message
     * @param message the message as a string
     * @return True if the message was sent through to the sink, false if it was filtered
     */
    boolean consume(long index, String message);
}
