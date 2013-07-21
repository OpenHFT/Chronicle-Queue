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

import java.io.IOException;
import java.nio.MappedByteBuffer;

/**
 * @author peter.lawrey
 */
public class VanillaMappedFileCache extends AbstractMappedFileCache {
    public VanillaMappedFileCache(String dirPath, ChronicleConfig config) throws IOException {
        super(dirPath, config);
    }

    @Override
    public MappedByteBuffer acquireMappedBufferForIndex(int indexNumber) throws IOException {
        throw new IOException();
    }

    @Override
    public void randomAccess(boolean randomAccess) {
    }

    @Override
    public void close() {
    }

    @Override
    public void roll() {
    }
}
