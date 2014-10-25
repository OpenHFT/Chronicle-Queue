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

package net.openhft.chronicle.tools;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author peter.lawrey
 */
public class MasterIndexFile {
    private final File file;
    private final List<String> filenames = new ArrayList<String>();

    public MasterIndexFile(File file) {
        this.file = file;
        loadFile();
    }

    private void loadFile() {
        try {
            if (!file.exists())
                return;
            BufferedReader br = new BufferedReader(new FileReader(file));
            try {
                for (String line; (line = br.readLine()) != null; ) {
                    filenames.add(line.trim().isEmpty() ? null : line);
                }
            } finally {
                br.close();
            }
        } catch (FileNotFoundException fnfe) {
            throw new AssertionError(fnfe);

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public String filenameFor(int index) {
        if (filenames.size() <= index)
            return null;
        return filenames.get(index);
    }

    public int append(String filenameAdded) throws IOException {
        int index = filenames.indexOf(filenameAdded);
        if (index >= 0)
            return index;
        filenames.add(filenameAdded);
        FileWriter fileWriter = new FileWriter(file);
        fileWriter.append(filenameAdded).append("\n");
        fileWriter.close();
        return filenames.size() - 1;
    }

    public void close() {
        // nothing to do.
    }

    public int size() {
        return filenames.size();
    }
}
