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
import net.openhft.chronicle.Excerpt;
import net.openhft.chronicle.ExcerptComparator;
import net.openhft.chronicle.tcp.ChronicleTcp;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.model.constraints.NotNull;

import java.nio.ByteBuffer;

public class WrappedExcerpts {

    public static class ByteBufferBytesExcerpt extends ByteBufferBytes implements Excerpt {
        public ByteBufferBytesExcerpt(@NotNull ByteBuffer buffer) {
            super(buffer);

            super.finished = true;
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
        public Chronicle chronicle() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean nextIndex() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long findMatch(@NotNull ExcerptComparator comparator) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void findRange(@NotNull long[] startEnd, @NotNull ExcerptComparator comparator) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean index(long l) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Excerpt toStart() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Excerpt toEnd() {
            throw new UnsupportedOperationException();
        }

        public static ByteBufferBytesExcerpt withSize(int size) {
            return new ByteBufferBytesExcerpt(ChronicleTcp.createBufferOfSize(size));
        }
    }

    //**************************************************************************
    //
    //**************************************************************************

    public static class ByteBufferBytesExcerptWrapper extends WrappedExcerpt {

        private final int defaulCapacity;

        public ByteBufferBytesExcerptWrapper(int defaulCapacity) {
            super(ByteBufferBytesExcerpt.withSize(defaulCapacity));

            this.defaulCapacity = defaulCapacity;
        }

        protected ByteBuffer buffer() {
            return ((ByteBufferBytesExcerpt)wrappedExcerpt).buffer();
        }

        protected ByteBufferBytesExcerpt excerpt() {
            return ((ByteBufferBytesExcerpt)wrappedExcerpt);
        }

        protected void resize(long capacity) {
            if(capacity > Integer.MAX_VALUE) {
                throw new IllegalStateException("Only capacities up to Integer.MAX_VALUE are supported");
            }

            if(capacity > excerpt().capacity()) {
                setExcerpt(ByteBufferBytesExcerpt.withSize((int) capacity));
            }

            excerpt().clear();
            excerpt().limit(capacity);
            buffer().clear();
            buffer().limit((int)capacity);
        }

        protected void cleanup() {
            excerpt().clear();
            buffer().clear();
        }
    }
}
