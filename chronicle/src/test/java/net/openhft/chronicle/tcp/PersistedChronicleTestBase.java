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

package net.openhft.chronicle.tcp;


import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleConfig;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.IndexedChronicle;
import net.openhft.chronicle.VanillaChronicle;
import net.openhft.chronicle.VanillaChronicleConfig;
import net.openhft.chronicle.tools.ChronicleTools;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PersistedChronicleTestBase {
    protected static final Logger LOGGER    = LoggerFactory.getLogger("PersistedChronicleTest");
    protected static final String TMP_DIR   = System.getProperty("java.io.tmpdir");
    protected static final String PREFIX    = "ch-persisted-";
    protected static final int    BASE_PORT = 12000;

    @Rule
    public final TestName testName = new TestName();

    // *************************************************************************
    //
    // *************************************************************************

    protected Chronicle localChronicleSink(final Chronicle chronicle, String host, int port) throws IOException {
        return new ChronicleSink(
            chronicle,
            ChronicleSinkConfig.DEFAULT.clone().sharedChronicle(true),
            host,
            port);
    }

    // *************************************************************************
    //
    // *************************************************************************

    protected synchronized String getIndexedTestPath() {
        final String path = TMP_DIR + "/" + PREFIX + testName.getMethodName();
        ChronicleTools.deleteOnExit(path);

        return path;
    }

    protected synchronized String getIndexedTestPath(String suffix) {
        final String path = TMP_DIR + "/" + PREFIX + testName.getMethodName() + suffix;
        ChronicleTools.deleteOnExit(path);

        return path;
    }

    protected ChronicleSource indexedChronicleSource(String basePath, int port) throws IOException {
        return new ChronicleSource(new IndexedChronicle(basePath), port);
    }

    protected ChronicleSource indexedChronicleSource(String basePath, int port, ChronicleConfig config) throws IOException {
        return new ChronicleSource(new IndexedChronicle(basePath, config), port);
    }

    protected static void assertIndexedClean(String path) {
        assertNotNull(path);
        assertTrue(new File(path + ".index").delete());
        assertTrue(new File(path + ".data").delete());
    }

    // *************************************************************************
    //
    // *************************************************************************

    protected synchronized String getVanillaTestPath() {
        final String path = TMP_DIR + "/" + PREFIX + testName.getMethodName();
        final File f = new File(path);
        if(f.exists()) {
            f.delete();
        }

        return path;
    }

    protected synchronized String getVanillaTestPath(String suffix) {
        final String path = TMP_DIR + "/" + PREFIX + testName.getMethodName() + suffix;
        final File f = new File(path);
        if(f.exists()) {
            f.delete();
        }

        return path;
    }

    protected Chronicle vanillaChronicleSource(String basePath, int port) throws IOException {
        return new ChronicleSource(new VanillaChronicle(basePath), port);
    }

    protected Chronicle vanillaChronicleSource(String basePath, int port, VanillaChronicleConfig config) throws IOException {
        return new ChronicleSource(new VanillaChronicle(basePath, config), port);
    }

    // *************************************************************************
    //
    // *************************************************************************

    public void testJira77(int port, Chronicle chronicleSrc, Chronicle chronicleTarget) throws IOException{
        final int BYTES_LENGTH = 66000;
        String basePath = getIndexedTestPath();
        ChronicleConfig config = ChronicleConfig.DEFAULT.clone();

        Random random = new Random();
        ChronicleSourceConfig sourceConfig = ChronicleSourceConfig.DEFAULT.clone();
        sourceConfig.minBufferSize(2 * BYTES_LENGTH);

        ChronicleSource chronicleSource = new ChronicleSource(chronicleSrc, sourceConfig, port);
        ChronicleSinkConfig sinkConfig = ChronicleSinkConfig.DEFAULT.clone();
        sinkConfig.minBufferSize(2 * BYTES_LENGTH);
        ChronicleSink chronicleSink = new ChronicleSink(chronicleTarget, sinkConfig, new InetSocketAddress(port));

        ExcerptAppender app = chronicleSrc.createAppender();
        byte[] bytes = new byte[BYTES_LENGTH];
        random.nextBytes(bytes);
        app.startExcerpt(4 + 4 + bytes.length);
        app.writeInt(bytes.length);
        app.write(bytes);
        app.finish();

        ExcerptTailer vtail = chronicleSink.createTailer();
        byte[] bytes2 = null;
        while (vtail.nextIndex()) {
            int bytesLength = vtail.readInt();
            bytes2 = new byte[bytesLength];
            vtail.read(bytes2);
            vtail.finish();
        }

        assertArrayEquals(bytes, bytes2);

        app.close();
        vtail.close();

        chronicleSrc.close();
        chronicleSource.close();
        chronicleSink.close();
    }
}
