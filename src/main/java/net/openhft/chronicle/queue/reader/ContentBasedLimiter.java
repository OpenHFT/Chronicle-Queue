/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.reader;

import net.openhft.chronicle.wire.DocumentContext;

/**
 * Interface for signaling when to halt reading from a queue based on the content of a message.
 * <p>This can be used to limit processing within the {@link ChronicleReader} based on specific conditions found in the messages.
 */
public interface ContentBasedLimiter {

    /**
     * Determines whether the {@link ChronicleReader} should stop processing further messages.
     * <p>This method examines the content of the next message and decides if reading should halt before processing it.
     *
     * @param documentContext The document context representing the next message to be processed
     * @return {@code true} to halt processing, {@code false} to continue processing the message
     */
    boolean shouldHaltReading(DocumentContext documentContext);

    /**
     * Configures the limiter with parameters before the reader begins processing.
     * <p>
     * This method allows the limiter to be customized using arguments provided via {@link Reader#limiterArg()}.
     *
     * @param reader The reader that is about to be executed, providing context and parameters for the limiter
     */
    void configure(Reader reader);
}
