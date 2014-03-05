/*
 * Copyright 2014 Peter Lawrey
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
package net.openhft.chronicle.osgi;

import org.ops4j.pax.exam.Option;

import java.io.File;

import static org.ops4j.pax.exam.CoreOptions.bundle;

/**
 *
 */
public class OSGiTestBase {
    /**
     *
     * @param projectName
     * @return
     */
    public static Option workspaceBundle(String projectName) {
        String baseDir = System.getProperty("main.basedir");
        String bundleDir = null;

        bundleDir = String.format("%s/%s/target/classes",baseDir,projectName);
        if(new File(bundleDir).exists()) {
            return bundle(String.format("reference:file:%s", bundleDir));
        }

        bundleDir = String.format("%s/../%s/target/classes",baseDir,projectName);
        if(new File(bundleDir).exists()) {
            return bundle(String.format("reference:file:%s", bundleDir));
        }

        return null;
    }
}
