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
import net.openhft.lang.io.WrappedBytes;
import net.openhft.lang.model.constraints.NotNull;

import java.io.StreamCorruptedException;

public class WrappedExcerptAppender<T extends ExcerptAppender> extends WrappedBytes<T> implements ExcerptAppender {

    public WrappedExcerptAppender(final @NotNull T appender) {
        super(appender);
    }

    public void startExcerpt() {
        wrapped.startExcerpt();
    }

    public void addPaddedEntry() {
        wrapped.addPaddedEntry();
    }

    public Chronicle chronicle() {
        return wrapped.chronicle();
    }

    public boolean wasPadding() {
        return wrapped.wasPadding();
    }

    public long index() {
        return wrapped.index();
    }

    public long lastWrittenIndex() {
        return wrapped.lastWrittenIndex();
    }

    public void startExcerpt(long capacity) {
        wrapped.startExcerpt(capacity);
    }

    public void nextSynchronous(boolean nextSynchronous) {
        wrapped.nextSynchronous(nextSynchronous);
    }

    public boolean nextSynchronous() {
        return wrapped.nextSynchronous();
    }

    @Override
    public boolean read8bitText(@NotNull StringBuilder stringBuilder) throws StreamCorruptedException {
        return wrapped.read8bitText(stringBuilder);
    }

    @Override
    public void write8bitText(CharSequence charSequence) {
        wrapped.write8bitText(charSequence);
    }
}
