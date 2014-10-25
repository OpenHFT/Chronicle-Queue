/*
 * Copyright 2014 Higher Frequency Trading
 * <p/>
 * http://www.higherfrequencytrading.com
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
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

    protected final Chronicle delegatedChronicle;

    public WrappedChronicle(final Chronicle delegatedChronicle) {
        this.delegatedChronicle = delegatedChronicle;
    }

    @Override
    public String name() {
        return this.delegatedChronicle != null ? this.delegatedChronicle.name() : "<noname>";
    }

    @Override
    public long lastIndex() {
        return this.delegatedChronicle != null ? this.delegatedChronicle.lastIndex() : -1;
    }

    @Override
    public long lastWrittenIndex() {
        return this.delegatedChronicle != null ? this.delegatedChronicle.lastWrittenIndex() : -1;
    }

    @Override
    public long size() {
        return this.delegatedChronicle != null ? this.delegatedChronicle.size() : -1;
    }

    @Override
    public void clear() {
        if(this.delegatedChronicle != null) {
            this.delegatedChronicle.clear();
        }
    }

    @Override
    public void close() throws IOException {
        if(this.delegatedChronicle != null) {
            this.delegatedChronicle.close();
        }
    }

    @Override
    public Excerpt createExcerpt() throws IOException {
        return this.delegatedChronicle != null ? this.delegatedChronicle.createExcerpt() : null;
    }

    @Override
    public ExcerptTailer createTailer() throws IOException {
        return this.delegatedChronicle != null ? this.delegatedChronicle.createTailer() : null;
    }

    @Override
    public ExcerptAppender createAppender() throws IOException {
        return this.delegatedChronicle != null ? this.delegatedChronicle.createAppender() : null;
    }
}
