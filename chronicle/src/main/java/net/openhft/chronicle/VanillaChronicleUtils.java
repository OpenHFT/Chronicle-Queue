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

package net.openhft.chronicle;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class VanillaChronicleUtils {

    public static final FileFilter IS_DIR = new FileFilter() {
        @Override
        public boolean accept(File pathname) {
            return pathname.isDirectory();
        }
    };

    /**
     *
     * @param cycleDir
     * @param name
     * @param forAppend
     * @return null if !forAppend and file does not exist
     * @throws IOException
     */
    public static File mkFiles(
            File cycleDir, String name, boolean forAppend) throws IOException {

        final File file = new File(cycleDir, name);

        if (!forAppend) {
            //This test needs to be done before any directories are created.
            if (!file.exists()) {
                return null;
            }
        }

        cycleDir.mkdirs();
        if(!file.exists() && !forAppend) {
            throw new FileNotFoundException(file.getAbsolutePath());
        }

        return file;
    }

    public static File indexFileFor(int cycle, int indexCount, VanillaDateCache dateCache) {
        return new File(
            dateCache.valueFor(cycle).path,
            VanillaIndexCache.FILE_NAME_PREFIX + indexCount
        );
    }

    public static File dataFileFor(int cycle, int threadId, int dataCount, VanillaDateCache dateCache) {
        return new File(
            dateCache.valueFor(cycle).path,
            VanillaDataCache.FILE_NAME_PREFIX + threadId + "-" + dataCount
        );
    }

    public static List<File> findLeafDirectories(File root) {
        final File[] files = root.listFiles(VanillaChronicleUtils.IS_DIR);
        if(files != null && files.length != 0) {
            List<File> leafs = new ArrayList<>();
            for(int i=files.length - 1; i >= 0; i--) {
                findLeafDirectories(leafs, files[i]);
            }

            return leafs;
        }

        return Collections.emptyList();
    }

    public static List<File> findLeafDirectories(List<File> leafs, File root) {
        final File[] files = root.listFiles(VanillaChronicleUtils.IS_DIR);
        if(files != null && files.length != 0) {
            for(int i=files.length - 1; i >= 0; i--) {
                findLeafDirectories(leafs, files[i]);
            }
        } else {
            leafs.add(root);
        }

        return leafs;
    }
}
