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
package org.slf4j.impl.chronicle;

import net.openhft.lang.io.IOTools;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 */
public class Slf4jChronicleTestBase {

    // *************************************************************************
    //
    // *************************************************************************

    public static final String BASEPATH =
        System.getProperty("java.io.tmpdir")
            + File.separator
            + "chronicle"
            + File.separator
            + new SimpleDateFormat("yyyyMMdd").format(new Date())
            + File.separator
            + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]
            + File.separator
            + "root";

    public static final String BASEPATH_LOGGER_RW =
        System.getProperty("java.io.tmpdir")
            + File.separator
            + "chronicle"
            + File.separator
            + new SimpleDateFormat("yyyyMMdd").format(new Date())
            + File.separator
            + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]
            + File.separator
            + "readwrite";

    public static final String BASEPATH_LOGGER_1 =
        System.getProperty("java.io.tmpdir")
            + File.separator
            + "chronicle"
            + File.separator
            + new SimpleDateFormat("yyyyMMdd").format(new Date())
            + File.separator
            + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]
            + File.separator
            + "logger_1";
}
