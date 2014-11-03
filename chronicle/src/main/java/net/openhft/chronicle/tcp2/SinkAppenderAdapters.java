/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.tcp2;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.IndexedChronicle;
import net.openhft.chronicle.VanillaChronicle;
import net.openhft.chronicle.tools.WrappedExcerptAppender;
import net.openhft.lang.model.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

class SinkAppenderAdapters {

    private static final Logger LOGGER = LoggerFactory.getLogger(SinkAppenderAdapters.class);

    /**
     * Creates a SinkAppenderAdapter.
     *
     * @param chronicle     the Chronicle
     * @return              the SinkAppenderAdapter
     * @throws IOException
     */
    public static SinkAppenderAdapter adapt(final @NotNull Chronicle chronicle) throws IOException {
        if(chronicle instanceof IndexedChronicle) {
            return new SinkAppenderAdapters.Indexed(chronicle, chronicle.createAppender());
        }

        if(chronicle instanceof VanillaChronicle) {
            return new SinkAppenderAdapters.Vanilla(chronicle, chronicle.createAppender());
        }

        throw new IllegalArgumentException("Can only adapt Indexed or Vanilla chronicles");
    }

    // *************************************************************************
    //
    // *************************************************************************

    private static final class Indexed extends WrappedExcerptAppender implements SinkAppenderAdapter {
        private final IndexedChronicle chronicle;

        public Indexed(@NotNull final Chronicle chronicle, @NotNull final ExcerptAppender appender) {
            super(appender);

            this.chronicle = (IndexedChronicle)chronicle;
        }

        @Override
        public boolean writePaddedEntry() {
            super.warappedAppender.addPaddedEntry();
            return true;
        }

        @Override
        public void startExcerpt(long capacity, long index) {
            super.warappedAppender.startExcerpt(capacity);
        }
    }

    private static final class Vanilla extends WrappedExcerptAppender implements SinkAppenderAdapter {
        private final VanillaChronicle chronicle;
        private final VanillaChronicle.VanillaAppender appender;

        public Vanilla(@NotNull final Chronicle chronicle, @NotNull final ExcerptAppender appender) {
            super(appender);

            this.chronicle = (VanillaChronicle)chronicle;
            this.appender = (VanillaChronicle.VanillaAppender)appender;
        }

        @Override
        public boolean writePaddedEntry() {
            LOGGER.warn("VanillaChronicle should not receive padded entries");
            return false;
        }

        @Override
        public void startExcerpt(long capacity, long index) {
            int cycle = (int) (index >>> chronicle.getEntriesForCycleBits());
            this.appender.startExcerpt(capacity, cycle);
        }
    }
}
