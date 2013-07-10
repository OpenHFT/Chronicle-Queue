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

package net.openhft.chronicle;

import net.openhft.lang.io.NativeBytes;

/**
 * @author peter.lawrey
 */
public class RollingNativeExcerptReader extends NativeBytes implements ExcerptReader {
    private final RollingChronicle chronicle;
    long index = -1;

    public RollingNativeExcerptReader(RollingChronicle chronicle) {
        super(0, 0, 0);
        this.chronicle = chronicle;
    }

    @Override
    public ExcerptReader toStart() {
        index = -1;
        return this;
    }

    @Override
    public ExcerptReader toEnd() {
        index = chronicle.size();
        return this;
    }

    @Override
    public boolean nextIndex() {
        return false;
    }

    @Override
    public boolean index(long l) {
        return false;
    }

    @Override
    public long index() {
        return index;
    }

    @Override
    public long size() {
        return chronicle.size();
    }

    @Override
    public Chronicle chronicle() {
        return chronicle;
    }
}
