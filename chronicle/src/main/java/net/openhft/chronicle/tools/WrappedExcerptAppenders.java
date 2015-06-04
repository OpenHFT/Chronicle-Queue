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
package net.openhft.chronicle.tools;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.tcp.ChronicleTcp;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.model.constraints.NotNull;

import java.nio.ByteBuffer;

public class WrappedExcerptAppenders {

    //**************************************************************************
    //
    //**************************************************************************

    public static class ByteBufferBytesAppender extends ByteBufferBytes implements ExcerptAppender {
        public ByteBufferBytesAppender(@NotNull ByteBuffer buffer) {
            super(buffer);

            super.finished = true;
        }

        @Override
        public void startExcerpt() {
            clear();
            buffer().clear();
        }

        @Override
        public void startExcerpt(long capacity) {
            if(capacity <= capacity()) {
                clear();
                buffer().clear();

                limit(capacity);
                buffer().limit((int) capacity);

            } else {
                throw new IllegalStateException("Excerpt's size can't exceed Excerpt's capacity");
            }
        }

        @Override
        public boolean wasPadding() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long index() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long lastWrittenIndex() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Chronicle chronicle() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addPaddedEntry() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean nextSynchronous() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void nextSynchronous(boolean nextSynchronous) {
            throw new UnsupportedOperationException();
        }

        public static ByteBufferBytesAppender withSize(int size) {
            return new ByteBufferBytesAppender(ChronicleTcp.createBufferOfSize(size));
        }
    }

    //**************************************************************************
    //
    //**************************************************************************

    public static class ByteBufferBytesExcerptAppenderWrapper extends WrappedExcerptAppender<ByteBufferBytesAppender> {

        private final int defaultCapacity;

        public ByteBufferBytesExcerptAppenderWrapper(int defaultCapacity) {
            super(ByteBufferBytesAppender.withSize(defaultCapacity));

            this.defaultCapacity = defaultCapacity;
        }

        @Override
        public void startExcerpt() {
            this.startExcerpt(this.defaultCapacity);
        }

        @Override
        public void startExcerpt(long capacity) {
            if(capacity > Integer.MAX_VALUE) {
                throw new IllegalStateException("Only capacities up to Integer.MAX_VALUE are supported");
            }

            if (capacity > wrapped.buffer().capacity()) {
                wrapped = ByteBufferBytesAppender.withSize((int) capacity);
            }

            super.startExcerpt(capacity);
        }
    }
}
