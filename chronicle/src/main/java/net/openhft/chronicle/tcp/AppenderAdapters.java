/*
 * Copyright 2015 Higher Frequency Trading
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

package net.openhft.chronicle.tcp;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.IndexedChronicle;
import net.openhft.chronicle.VanillaChronicle;
import net.openhft.lang.model.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AppenderAdapters {

    private static final Logger LOGGER = LoggerFactory.getLogger(AppenderAdapters.class);

    static final class IndexedAppenderAdapter extends AppenderAdapter {
        public IndexedAppenderAdapter(
                @NotNull final Chronicle chronicle,
                @NotNull final ExcerptAppender appender) {
            super(appender);
        }

        @Override
        public void writePaddedEntry() {
            super.wrapped.addPaddedEntry();
        }

        @Override
        public void startExcerpt(long capacity, long index) {
            super.wrapped.startExcerpt(capacity);
        }
    }

    static final class VanillaAppenderAdapter extends AppenderAdapter {
        private final VanillaChronicle chronicle;
        private final VanillaChronicle.VanillaAppender appender;

        public VanillaAppenderAdapter(
                @NotNull final Chronicle chronicle,
                @NotNull final ExcerptAppender appender) {
            super(appender);

            this.chronicle = (VanillaChronicle) chronicle;
            this.appender = (VanillaChronicle.VanillaAppender) appender;
        }

        @Override
        public void writePaddedEntry() {
            LOGGER.warn("VanillaChronicle should not receive padded entries");
        }

        @Override
        public void startExcerpt(long capacity, long index) {
            int cycle = (int) (index >>> chronicle.getEntriesForCycleBits());
            this.appender.startExcerpt(capacity, cycle);
        }
    }

    // *************************************************************************
    //
    // *************************************************************************

    public static AppenderAdapter createAdapter(Chronicle chronicle) throws IOException {
        if (chronicle instanceof IndexedChronicle) {
            return new AppenderAdapters.IndexedAppenderAdapter(
                    chronicle,
                    chronicle.createAppender());
        }

        if (chronicle instanceof VanillaChronicle) {
            return new AppenderAdapters.VanillaAppenderAdapter(
                    chronicle,
                    chronicle.createAppender());
        }

        throw new IllegalArgumentException("Can only adapt Indexed or Vanilla chronicles");
    }
}
