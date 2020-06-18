/*
 * Copyright 2016-2020 Chronicle Software
 *
 * https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.wire.MessageHistory;

public enum NoMessageHistory implements MessageHistory {
    INSTANCE;

    @Override
    public int timings() {
        return 0;
    }

    @Override
    public long timing(int n) {
        return -1;
    }

    @Override
    public int sources() {
        return 0;
    }

@Override
    public int sourceId(int n) {
        return -1;
    }

    @Override
    public boolean sourceIdsEndsWith(int[] sourceIds) {
        return false;
    }

    @Override
    public long sourceIndex(int n) {
        return -1;
    }

    @Override
    public void reset(int sourceId, long sourceIndex) {
        // ignored
    }

    public void reset() {
        // no-op
    }

    @Override
    public int lastSourceId() {
        return -1;
    }

    @Override
    public long lastSourceIndex() {
        return -1;
    }

}
