/*
 * Copyright 2016-2025 chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue.util;

import net.openhft.chronicle.bytes.PageUtil;
import net.openhft.chronicle.core.Jvm;
import org.junit.rules.TestName;

import java.io.File;
import java.nio.file.Paths;

public final class HugetlbfsTestUtil {

    public static final String HUGETLBFS_PATH_PROPERTY = "chronicle.queue.tests.hugetlbfsPath";

    public static final String HUGETLBFS_PATH = Jvm.getProperty(HUGETLBFS_PATH_PROPERTY, "/mnt/huge");

    private HugetlbfsTestUtil() {
    }

    /**
     * @return if hugetlbfs file system is available at /huge or at the path specified by system property with key
     * {@link #HUGETLBFS_PATH_PROPERTY}.
     */
    public static boolean isHugetlbfsAvailable() {
        String hugetlbfsPath = hugetlbfsPath();
        if (hugetlbfsPath != null) {
            return PageUtil.isHugePage(hugetlbfsPath);
        } else {
            return false;
        }
    }

    /**
     * Resolve the path to hugetlbfs on this file system
     *
     * @return The value of {@link #HUGETLBFS_PATH} if it exists, otherwise null.
     */
    public static String hugetlbfsPath() {
        if (new File(HUGETLBFS_PATH).exists()) {
            return HUGETLBFS_PATH;
        } else {
            return null;
        }
    }

    /**
     * Get a path to a queue directory on hugetlbfs.
     *
     * @param testName Junit {@link TestName} rule that contains the unit test method name.
     * @return a unique path on hugetlbfs scoped for this test method name.
     */
    public static String getHugetlbfsQueueDirectory(TestName testName) {
        String hugetlbfsPath = hugetlbfsPath();
        if (hugetlbfsPath == null || !isHugetlbfsAvailable()) {
            throw new IllegalStateException("hugetlbfs is not available");
        }

        return Paths.get(hugetlbfsPath, testName.getMethodName()).toString();
    }
}
