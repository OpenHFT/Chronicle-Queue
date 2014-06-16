/*
 * Copyright 2013 Peter Lawrey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
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
import net.openhft.lang.io.DirectBytes;
import net.openhft.lang.io.DirectStore;
import net.openhft.lang.io.NativeBytes;
import net.openhft.lang.model.constraints.NotNull;

public class SafeExcerptAppender extends NativeBytes implements ExcerptAppender {
    private final ExcerptAppender appender;
    private final long bufferSize;
    private DirectBytes buffer;

    public SafeExcerptAppender(final @NotNull ExcerptAppender appender, long bufferSize) {
        this(appender, bufferSize, true);
    }

    public SafeExcerptAppender(final @NotNull ExcerptAppender appender, long bufferSize, boolean leazyAllocation) {
        super(NO_PAGE, NO_PAGE);

        this.appender = appender;
        this.bufferSize = bufferSize;
        this.buffer = null;

        if(!leazyAllocation) {
            allocate();
        }
    }

    // *************************************************************************
    //
    // *************************************************************************

    @Override
    public void startExcerpt() {
        startExcerpt(this.bufferSize);
    }

    @Override
    public void startExcerpt(long capacity) {
        assert capacity <= this.bufferSize;
        allocate();
        buffer.positionAndSize(0,capacity);
    }

    @Override
    public void finish() {
        if(this.startAddr != NO_PAGE) {
            appender.startExcerpt(positionAddr - startAddr);
            appender.write(buffer);
            appender.finish();

            reset();
        }
    }

    @Override
    public void close() {
        this.appender.close();

        if(this.buffer != null) {
            this.buffer.release();

            this.startAddr = NO_PAGE;
            this.limitAddr = NO_PAGE;
            this.capacityAddr = NO_PAGE;
            this.positionAddr = NO_PAGE;
        }
    }

    // *************************************************************************
    //
    // *************************************************************************

    @Override
    public boolean wasPadding() {
        return appender.wasPadding();
    }

    @Override
    public long index() {
        return appender.index();
    }

    @Override
    public long lastWrittenIndex() {
        return appender.lastWrittenIndex();
    }

    @Override
    public ExcerptAppender toEnd() {
        appender.toEnd();
        return this;
    }

    @Override
    public Chronicle chronicle() {
        return appender.chronicle();
    }

    @Override
    public void addPaddedEntry() {
        appender.addPaddedEntry();
    }

    @Override
    public boolean nextSynchronous() {
        return appender.nextSynchronous();
    }

    @Override
    public void nextSynchronous(boolean nextSynchronous) {
        appender.nextSynchronous(nextSynchronous);
    }

    // *************************************************************************
    //
    // *************************************************************************

    private void allocate() {
        if(this.startAddr == NO_PAGE && buffer == null) {
            buffer = DirectStore.allocateLazy(this.bufferSize).bytes();
            startAddr = buffer.address();
            limitAddr = startAddr + buffer.capacity();

            reset();
        }
    }

    private void reset() {
        if(this.startAddr != NO_PAGE && buffer != null) {
            buffer.positionAndSize(0,this.bufferSize);
            positionAddr = buffer.address();
            capacityAddr = limitAddr;
        }
    }
}
