/*
 * Copyright 2014 Higher Frequency Trading
 * <p>
 * http://www.higherfrequencytrading.com
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.tools;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.Excerpt;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;

import java.io.IOException;

public class WrappedChronicle implements Chronicle {

    protected final Chronicle wrappedChronicle;

    public WrappedChronicle(final Chronicle wrappedChronicle) {
        this.wrappedChronicle = wrappedChronicle;
    }

    @Override
    public String name() {
        return this.wrappedChronicle != null ? this.wrappedChronicle.name() : "<noname>";
    }

    @Override
    public long lastIndex() {
        return this.wrappedChronicle != null ? this.wrappedChronicle.lastIndex() : -1;
    }

    @Override
    public long lastWrittenIndex() {
        return this.wrappedChronicle != null ? this.wrappedChronicle.lastWrittenIndex() : -1;
    }

    @Override
    public long size() {
        return this.wrappedChronicle != null ? this.wrappedChronicle.size() : -1;
    }

    @Override
    public void clear() {
        if(this.wrappedChronicle != null) {
            this.wrappedChronicle.clear();
        }
    }

    @Override
    public void close() throws IOException {
        if(this.wrappedChronicle != null) {
            this.wrappedChronicle.close();
        }
    }

    @Override
    public Excerpt createExcerpt() throws IOException {
        return this.wrappedChronicle != null ? this.wrappedChronicle.createExcerpt() : null;
    }

    @Override
    public ExcerptTailer createTailer() throws IOException {
        return this.wrappedChronicle != null ? this.wrappedChronicle.createTailer() : null;
    }

    @Override
    public ExcerptAppender createAppender() throws IOException {
        return this.wrappedChronicle != null ? this.wrappedChronicle.createAppender() : null;
    }
}
