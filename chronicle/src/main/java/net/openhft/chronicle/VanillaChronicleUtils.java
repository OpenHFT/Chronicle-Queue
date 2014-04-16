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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class VanillaChronicleUtils {
    private static final Logger LOGGER = Logger.getLogger(VanillaChronicleUtils.class.getName());

    public static File mkFiles(String basePath, String cycleStr, String name, boolean forAppend) throws IOException {
        File dir = new File(basePath, cycleStr);

        if (!forAppend) {
            //This test needs to be done before any directories are created.
            File f = new File(dir, name);
            if (!f.exists()) {
                throw new FileNotFoundException(f.getAbsolutePath());
            }
        }

        if (!dir.isDirectory()) {
            boolean created = dir.mkdirs();
            if (LOGGER.isLoggable(Level.FINE))
                LOGGER.fine("Created " + dir + " is " + created);
        }

        File file = new File(dir, name);
        if (file.exists()) {
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("Opening " + file);
            }
        } else if (forAppend) {
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("Creating " + file);
            }
        } else {
            throw new FileNotFoundException(file.getAbsolutePath());
        }

        return file;
    }
}
