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
package net.openhft.chronicle.tcp;


import net.openhft.lang.model.constraints.NotNull;

public class ChronicleSinkConfig implements Cloneable {
    public static final ChronicleSinkConfig DEFAULT = new ChronicleSinkConfig();

    private int minBufferSize;
    private long reconnectDelay;

    private ChronicleSinkConfig() {
        minBufferSize = 256 * 1024;
        reconnectDelay = 500; // milliseconds
    }

    // *************************************************************************
    //
    // *************************************************************************

    public ChronicleSinkConfig minBufferSize(int minBufferSize) {
        this.minBufferSize = minBufferSize;
        return this;
    }

    public int minBufferSize() {
        return this.minBufferSize;
    }

    public ChronicleSinkConfig reconnectDelay(long reconnectDelay) {
        this.reconnectDelay = reconnectDelay;
        return this;
    }

    public long reconnectDelay() {
        return this.minBufferSize;
    }

    // *************************************************************************
    //
    // *************************************************************************

    @NotNull
    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    @Override
    public ChronicleSinkConfig clone() {
        try {
            return (ChronicleSinkConfig) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }
}
