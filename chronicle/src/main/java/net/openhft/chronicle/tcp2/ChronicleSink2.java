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
package net.openhft.chronicle.tcp2;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.Excerpt;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;

import java.io.IOException;

public class ChronicleSink2 implements Chronicle {
    private final Chronicle chronicle;
    private final ChronicleSink2Support.TcpSink tcpSink;

    public ChronicleSink2(final Chronicle chronicle, final ChronicleSink2Support.TcpSink tcpSink) {
        this.tcpSink = tcpSink;
        this.chronicle = chronicle;
    }

    // *************************************************************************
    //
    // *************************************************************************

    @Override
    public String name() {
        return this.chronicle != null ? this.chronicle.name() : "<noname>";
    }

    @Override
    public long lastWrittenIndex() {
        return this.chronicle != null ? this.chronicle.lastWrittenIndex() : -1;
    }

    @Override
    public long size() {
        return this.chronicle != null ? this.chronicle.size() : -1;
    }

    @Override
    public void clear() {
        // TODO: check that clear is not invoked on a running chronicle
        if(this.chronicle != null) {
            this.chronicle.clear();
        }
    }

    @Override
    public void close() throws IOException {
        if(this.tcpSink != null) {
            this.tcpSink.close();
        }

        if(this.chronicle != null) {
            this.chronicle.close();
        }
    }

    // *************************************************************************
    //
    // *************************************************************************

    @Override
    public Excerpt createExcerpt() throws IOException {
        return null;
    }

    @Override
    public ExcerptTailer createTailer() throws IOException {
        return null;
    }

    @Override
    public ExcerptAppender createAppender() throws IOException {
        throw new UnsupportedOperationException();
    }

    // *************************************************************************
    //
    // *************************************************************************
}
