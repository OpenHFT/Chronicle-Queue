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

import net.openhft.lang.io.IOTools;
import org.apache.commons.lang3.StringUtils;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;

public class VanillaChronicleTestBase {
    protected static final String TMP_DIR = System.getProperty("java.io.tmpdir");

    @Rule
    public final TestName testName = new TestName();

    protected synchronized String getTestPath() {
        final String path = TMP_DIR + "/vc-" + testName.getMethodName();
        IOTools.deleteDir(path);

        return path;
    }

    protected synchronized String getTestPath(String suffix) {
        final String path = TMP_DIR + "/vc-" + testName.getMethodName() + suffix;
        IOTools.deleteDir(path);

        return path;
    }

    protected int getPID() {
        return Integer.parseInt(getPIDAsString());
    }

    protected String getPIDAsString() {
        final String name = ManagementFactory.getRuntimeMXBean().getName();
        return name.split("@")[0];
    }

    protected void sleep(long timeout, TimeUnit unit) {
        sleep(TimeUnit.MILLISECONDS.convert(timeout,unit));
    }

    protected void sleep(long timeout) {
        try {
            Thread.sleep(timeout);
        } catch (InterruptedException e) {
        }
    }

    public void lsof(final String pid) throws Exception {
        lsof(pid, null);
    }

    public void lsof(final String pid, final String pattern) throws Exception {
        String cmd = null;
        if(new File("/usr/sbin/lsof").exists()) {
            cmd = "/usr/sbin/lsof";
        } else if(new File("/usr/bin/lsof").exists()) {
            cmd = "/usr/bin/lsof";
        }

        if(cmd != null) {
            final ProcessBuilder pb = new ProcessBuilder(cmd, "-p", pid);
            final Process proc = pb.start();
            final BufferedReader br = new BufferedReader(new InputStreamReader(proc.getInputStream()));

            String line;
            while((line = br.readLine()) != null) {
                if(StringUtils.isBlank(pattern) || line.matches(pattern) || line.contains(pattern)) {
                    System.out.println(line);
                }
            }

           br.close();
           proc.destroy();
        }
    }
}
