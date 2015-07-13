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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class VanillaChronicleUtils {

    public static File mkFiles(
            String basePath, String cycleStr, String name, boolean forAppend) throws IOException {

        final File dir = new File(basePath, cycleStr);
        final File file = new File(dir, name);

        if (!forAppend) {
            //This test needs to be done before any directories are created.
            if (!file.exists()) {
                throw new FileNotFoundException(file.getAbsolutePath());
            }
        }

        dir.mkdirs();
        if(!file.exists() && !forAppend) {
            throw new FileNotFoundException(file.getAbsolutePath());
        }

        return file;
    }

    public static File fileFor(
            String basePath, int cycle, int indexCount, VanillaDateCache dateCache) throws IOException {
        return new File(
            new File(basePath, dateCache.formatFor(cycle)),
            VanillaIndexCache.FILE_NAME_PREFIX + indexCount);
    }


    public static List<File> findLeafDirectories(File root) {
        final List<File> files =  findLeafDirectories(new ArrayList<File>(), root);
        files.remove(root);

        return files;
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


    public static final FileFilter IS_DIR = new FileFilter() {
        @Override
        public boolean accept(File pathname) {
            return pathname.isDirectory();
        }
    };
}
