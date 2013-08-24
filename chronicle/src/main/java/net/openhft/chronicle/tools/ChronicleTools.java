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

import net.openhft.chronicle.ChronicleConfig;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.IndexedChronicle;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * @author peter.lawrey
 */
public enum ChronicleTools {
    ;

    /**
     * Delete a chronicle now and on exit, for testing
     *
     * @param basePath of the chronicle
     */
    public static void deleteOnExit(String basePath) {
        for (String name : new String[]{basePath + ".data", basePath + ".index"}) {
            File file = new File(name);
            //noinspection ResultOfMethodCallIgnored
            file.delete();
            file.deleteOnExit();
        }
    }

    public static void deleteDirOnExit(String dirPath) {
        DeleteStatic.INSTANCE.add(dirPath);
    }

    enum DeleteStatic {
        INSTANCE;
        @SuppressWarnings("TypeMayBeWeakened")
        final Set<String> toDeleteList = new LinkedHashSet<String>();

        {
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    for (String dir : toDeleteList) {
                        System.out.println("Deleting " + dir);
                        deleteDir(dir);
                    }
                }
            }));
        }

        synchronized void add(String dirPath) {
            deleteDir(dirPath);
            toDeleteList.add(dirPath);
        }

        private void deleteDir(String dirPath) {
            File dir = new File(dirPath);
            // delete one level.
            if (dir.isDirectory()) {
                File[] files = dir.listFiles();
                if (files != null)
                    for (File file : files)
                        file.delete();
            }
            dir.delete();
        }
    }

    public static void warmup() {
        boolean done = ChronicleWarmup.DONE;
    }
}

class ChronicleWarmup {
    public static final boolean DONE;
    public static final int WARMUP_ITER = 200000;
    private static final String TMP = System.getProperty("java.io.tmpdir");

    static {
        ChronicleConfig cc = ChronicleConfig.DEFAULT.clone();
        cc.dataBlockSize(64);
        cc.indexBlockSize(64);
        String basePath = TMP + "/warmup-" + Math.random();
        ChronicleTools.deleteOnExit(basePath);
        try {
            IndexedChronicle ic = new IndexedChronicle(basePath, cc);
            ExcerptAppender appender = ic.createAppender();
            ExcerptTailer tailer = ic.createTailer();
            for (int i = 0; i < WARMUP_ITER; i++) {
                appender.startExcerpt(4);
                appender.writeInt(i);
                appender.finish();
                boolean b = tailer.nextIndex() || tailer.nextIndex();
                tailer.readInt();
                tailer.finish();
            }
            ic.close();
            System.gc();
            DONE = true;
        } catch (IOException e) {
            throw new AssertionError();
        }
    }
}
