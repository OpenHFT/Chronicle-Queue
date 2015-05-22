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

import net.openhft.chronicle.*;
import net.openhft.lang.io.WrappedBytes;
import net.openhft.lang.model.constraints.NotNull;

/**
 * @author peter.lawrey
 */
public class WrappedExcerpt extends WrappedBytes implements ExcerptTailer, ExcerptAppender, Excerpt, MappingProvider<WrappedExcerpt> {
    protected ExcerptTailer wrappedTailer;
    protected ExcerptAppender wrappedAppender;
    protected ExcerptCommon wrappedCommon;
    protected Excerpt wrappedExcerpt;
    private MappingFunction withMapping;

    public WrappedExcerpt(ExcerptCommon excerptCommon) {
        super(excerptCommon);
        setExcerpt(excerptCommon);
    }

    public WrappedExcerpt withMapping(MappingFunction mapping) {
        this.withMapping = mapping;
        return this;
    }

    public MappingFunction withMapping() {
        return this.withMapping;
    }

    protected void setExcerpt(ExcerptCommon excerptCommon) {
        wrappedTailer   = excerptCommon instanceof ExcerptTailer ? (ExcerptTailer) excerptCommon : null;
        wrappedAppender = excerptCommon instanceof ExcerptAppender ? (ExcerptAppender) excerptCommon : null;
        wrappedExcerpt  = excerptCommon instanceof Excerpt ? (Excerpt) excerptCommon : null;
        wrappedCommon   = excerptCommon;
    }

    @Override
    public Chronicle chronicle() {
        return wrappedCommon.chronicle();
    }

    @Override
    public boolean nextIndex() {
        return wrappedTailer.nextIndex();
    }

    @Override
    public boolean index(long index) throws IndexOutOfBoundsException {
        return wrappedExcerpt == null ? wrappedTailer.index(index) : wrappedExcerpt.index(index);
    }

    @Override
    public void startExcerpt() {
        wrappedAppender.startExcerpt();
    }

    @Override
    public void startExcerpt(long capacity) {
        wrappedAppender.startExcerpt(capacity);
    }

    @Override
    public void addPaddedEntry() {
        wrappedAppender.addPaddedEntry();
    }

    @Override
    public boolean nextSynchronous() {
        return wrappedAppender.nextSynchronous();
    }

    @Override
    public void nextSynchronous(boolean nextSynchronous) {
        wrappedAppender.nextSynchronous();
    }

    @Override
    public long index() {
        return wrappedCommon.index();
    }

    @Override
    public long lastWrittenIndex() {
        return wrappedAppender.lastWrittenIndex();
    }

    @NotNull
    @Override
    public Excerpt toStart() {
        if (wrappedTailer == null) {
            wrappedExcerpt.toStart();

        } else {
            wrappedTailer.toStart();
        }

        return this;
    }

    @NotNull
    @Override
    public Excerpt toEnd() {
        wrappedTailer.toEnd();
        return this;
    }

    @Override
    public boolean wasPadding() {
        return wrappedCommon.wasPadding();
    }

    @Override
    public long findMatch(@NotNull ExcerptComparator comparator) {
        return wrappedExcerpt.findMatch(comparator);
    }

    @Override
    public void findRange(@NotNull long[] startEnd, @NotNull ExcerptComparator comparator) {
        wrappedExcerpt.findRange(startEnd, comparator);
    }
}
