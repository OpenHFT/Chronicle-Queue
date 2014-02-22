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
package net.openhft.chronicle.sandbox.tools;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.sandbox.BytesProcessor;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;
import java.util.logging.Logger;

/**
 *
 */
public class ChronicleProcessor implements Runnable, Closeable {
    private static final Logger LOGGER = Logger.getLogger(ChronicleProcessor.class.getName());

    private final Chronicle chronicle;
    private final BytesProcessor processor;

    /**
     *
     * @param chronicle
     * @param processor
     */
    public ChronicleProcessor(
        @NotNull final Chronicle chronicle,
        @NotNull final BytesProcessor processor) {
        this.chronicle = chronicle;
        this.processor = processor;
    }

    @Override
    public void run() {
        ExcerptTailer tailer = null;

        try {
            tailer = this.chronicle.createTailer();

            while(tailer.nextIndex()) {
                processor.process(tailer);
            }
        } catch (IOException e) {
            LOGGER.warning(e.getMessage());
        } finally {
            if(tailer != null) {
                tailer.close();
            }
        }
    }

    @Override
    public void close() throws IOException {
        if(this.chronicle != null) {
            this.chronicle.close();
        }
    }
}
