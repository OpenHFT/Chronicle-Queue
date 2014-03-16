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

import net.openhft.lang.io.MappedFile;
import net.openhft.lang.model.constraints.NotNull;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * User: peter.lawrey Date: 13/08/13 Time: 18:58
 */
public class MapUtils {
    @Deprecated()

    public static MappedByteBuffer getMap(@NotNull FileChannel fileChannel, long start, int size) throws IOException {
        return MappedFile.getMap(fileChannel, start, size);
    }

}
