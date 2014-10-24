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

package net.openhft.chronicle.tcp;


import net.openhft.lang.model.constraints.NotNull;

public class ChronicleSourceConfig implements Cloneable {
    public static final ChronicleSourceConfig DEFAULT = new ChronicleSourceConfig();

    private int maxMessages;
    private int minBufferSize;
    private long heartbeatInterval;
    private long selectTimeout;

    private ChronicleSourceConfig() {
        minBufferSize = 256 * 1024;
        heartbeatInterval = 2500;
        maxMessages = 128;
        selectTimeout = 1000;
    }

    // *************************************************************************
    //
    // *************************************************************************

    public ChronicleSourceConfig maxMessages(int maxMessages) {
        this.maxMessages = maxMessages;
        return this;
    }

    public int maxMessages() {
        return this.maxMessages;
    }

    public ChronicleSourceConfig minBufferSize(int minBufferSize) {
        this.minBufferSize = minBufferSize;
        return this;
    }

    public int minBufferSize() {
        return this.minBufferSize;
    }

    public ChronicleSourceConfig heartbeatInterval(long heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
        return this;
    }

    public long heartbeatInterval() {
        return this.heartbeatInterval;
    }

    public ChronicleSourceConfig selectTimeout(long selectTimeout) {
        if(selectTimeout < 0) {
            throw new IllegalArgumentException("SelectTimeout must be >= 0");
        }

        this.selectTimeout = selectTimeout;
        return this;
    }

    public long selectTimeout() {
        return this.selectTimeout;
    }

    // *************************************************************************
    //
    // *************************************************************************

    @NotNull
    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    @Override
    public ChronicleSourceConfig clone() {
        try {
            return (ChronicleSourceConfig) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }
}
