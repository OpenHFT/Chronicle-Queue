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
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.lang.io.WrappedBytes;
import net.openhft.lang.model.constraints.NotNull;

import java.io.StreamCorruptedException;

public class WrappedExcerptTailer extends WrappedBytes<ExcerptTailer> implements ExcerptTailer {
    protected ExcerptTailer wrappedTailer;

    public WrappedExcerptTailer(@NotNull ExcerptTailer tailer) {
        super(tailer);
    }

    @Override
    public Chronicle chronicle() {
        return wrappedTailer.chronicle();
    }

    @Override
    public boolean nextIndex() {
        return wrappedTailer.nextIndex();
    }

    @Override
    public boolean index(long index) throws IndexOutOfBoundsException {
        return wrappedTailer.index(index);
    }

    @Override
    public long index() {
        return wrappedTailer.index();
    }

    @NotNull
    @Override
    public ExcerptTailer toStart() {
        wrappedTailer.toStart();
        return this;
    }

    @NotNull
    @Override
    public ExcerptTailer toEnd() {
        wrappedTailer.toEnd();
        return this;
    }

    @Override
    public boolean wasPadding() {
        return wrappedTailer.wasPadding();
    }

    @Override
    public boolean read8bitText(@NotNull StringBuilder stringBuilder) throws StreamCorruptedException {
        return wrappedTailer.read8bitText(stringBuilder);
    }

    @Override
    public void write8bitText(CharSequence charSequence) {
        wrappedTailer.write8bitText(charSequence);
    }
}
