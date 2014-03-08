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

package net.openhft.chronicle.tools;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * @author peter.lawrey
 */
public enum DailingRollingIndexReader {
    ;

    private static void dumpMaster(File dir, PrintWriter writer) {
        MasterIndexFile mif = new MasterIndexFile(new File(dir, "master"));
        writer.println(dir.getName() + ": " + mif.size());
        for (int i = 0; i < mif.size(); i++)
            writer.println("\t" + i + "\t" + mif.filenameFor(i));
        mif.close();
    }

    public static String masterToString(File dir) {
        StringWriter sw = new StringWriter();
        dumpMaster(dir, new PrintWriter(sw));
        return sw.toString();
    }

    public static void main(String... args) {
        PrintWriter pw = new PrintWriter(System.out);
        dumpMaster(new File(args[0]), pw);
    }
}
