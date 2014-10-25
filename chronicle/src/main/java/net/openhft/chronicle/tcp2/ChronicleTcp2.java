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
import net.openhft.lang.model.constraints.NotNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ChronicleTcp2 {
    public static final int HEADER_SIZE = 12;
    public static final int INITIAL_BUFFER_SIZE = 64 * 1024;
    public static final int IN_SYNC_LEN = -128;
    public static final int PADDED_LEN = -127;
    public static final int SYNC_IDX_LEN = -126;
    public static final long ACTION_SUBSCRIBE = 1;
    public static final long ACTION_QUERY     = 2;

    public static ByteBuffer createBuffer(int minSize, ByteOrder byteOrder) {
        int newSize = (minSize + INITIAL_BUFFER_SIZE - 1) / INITIAL_BUFFER_SIZE * INITIAL_BUFFER_SIZE;
        return ByteBuffer.allocateDirect(newSize).order(byteOrder);
    }

    public static String connectionName(String name, final InetSocketAddress bindAddress, final InetSocketAddress connectAddress) {
        StringBuilder sb = new StringBuilder(name);
        if (bindAddress != null && connectAddress != null) {
            sb.append("[");
            sb.append(bindAddress.toString());
            sb.append(" -> ");
            sb.append(connectAddress.toString());
            sb.append("]");
        } else if (bindAddress != null) {
            sb.append("[");
            sb.append(bindAddress.toString());
            sb.append("]");
        } else if (connectAddress != null) {
            sb.append("[");
            sb.append(connectAddress.toString());
            sb.append("]");
        }

        return sb.toString();
    }

    // *************************************************************************
    //
    // *************************************************************************

    public static interface AppenderAdapter {
        public boolean handlePadding();
        public void startExcerpt(long capacity, long index);
    }

    public static class IndexedAppenderAdaper implements AppenderAdapter {
        private final IndexedChronicle chronicle;
        private final ExcerptAppender appender;

        public IndexedAppenderAdaper(@NotNull final Chronicle chronicle, @NotNull final ExcerptAppender appender) {
            this.chronicle = (IndexedChronicle)chronicle;
            this.appender = appender;
        }

        @Override
        public boolean handlePadding() {
            appender.startExcerpt(chronicle.config().dataBlockSize() - 1);
            return true;
        }

        @Override
        public void startExcerpt(long capacity, long index) {
            this.appender.startExcerpt(capacity);
        }
    }

    public static class VanillaAppenderAdaper implements AppenderAdapter {
        private final VanillaChronicle chronicle;
        private final VanillaChronicle.VanillaAppender appender;

        public VanillaAppenderAdaper(@NotNull final Chronicle chronicle, @NotNull final ExcerptAppender appender) {
            this.chronicle = (VanillaChronicle)chronicle;
            this.appender = (VanillaChronicle.VanillaAppender)appender;
        }

        @Override
        public boolean handlePadding() {
            return false;
        }

        @Override
        public void startExcerpt(long capacity, long index) {
            int cycle = (int) (index >>> chronicle.getEntriesForCycleBits());
            this.appender.startExcerpt(capacity, cycle);
        }
    }
}

