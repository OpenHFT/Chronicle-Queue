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

import net.openhft.chronicle.*;
import net.openhft.lang.io.IOTools;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
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
            // noinspection ResultOfMethodCallIgnored
            file.delete();
            file.deleteOnExit();
        }
    }

    public static void deleteDirOnExit(String dirPath) {
        DeleteStatic.INSTANCE.add(dirPath);
    }

    @NotNull
    public static String asString(@NotNull ByteBuffer bb) {
        StringBuilder sb = new StringBuilder();
        for (int i = bb.position(); i < bb.limit(); i++) {
            byte b = bb.get(i);
            if (b < ' ') {
                int h = b & 0xFF;
                if (h < 16)
                    sb.append('0');
                sb.append(Integer.toHexString(h));
            } else {
                sb.append(' ').append((char) b);
            }
        }
        return sb.toString();
    }

    /**
     * Take a text copy of the contents of the Excerpt without changing it's position. Can be called in the debugger.
     *
     * @param excerpt to get text from
     * @return 256 bytes as text with `.` replacing special bytes.
     */
    @NotNull
    public static String asString(@NotNull ExcerptCommon excerpt) {
        return asString(excerpt, excerpt.position());
    }

    /**
     * Take a text copy of the contents of the Excerpt without changing it's position. Can be called in the debugger.
     *
     * @param excerpt  to get text from
     * @param position the position to get text from
     * @return up to 1024 bytes as text with `.` replacing special bytes.
     */
    @NotNull
    private static String asString(@NotNull ExcerptCommon excerpt, long position) {
        return asString(excerpt, position, 1024);
    }

    /**
     * Take a text copy of the contents of the Excerpt without changing it's position. Can be called in the debugger.
     *
     * @param excerpt  to get text from
     * @param position the position to get text from
     * @param length   the maximum length
     * @return length bytes as text with `.` replacing special bytes.
     */
    @NotNull
    private static String asString(@NotNull ExcerptCommon excerpt, long position, long length) {
        long limit = Math.min(position + length, excerpt.capacity());
        StringBuilder sb = new StringBuilder((int) (limit - position));
        for (long i = position; i < limit; i++) {
            char ch = (char) excerpt.readUnsignedByte(i);
            if (ch < ' ' || ch > 127)
                ch = '.';
            sb.append(ch);
        }
        return sb.toString();
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
                        // TODO no idea why the // is needed. Appears to be a bug in the JVM.
                        System.out.println("Deleting " + dir.replaceAll("/", "//"));
                        IOTools.deleteDir(dir);
                    }
                }
            }));
        }

        synchronized void add(String dirPath) {
            IOTools.deleteDir(dirPath);
            toDeleteList.add(dirPath);
        }

    }

    public static void warmup() {
        //noinspection UnusedDeclaration needed to laod class.
        boolean done = ChronicleWarmup.DONE;
    }
}

class ChronicleWarmup {
    public static final boolean DONE;
    private static final int WARMUP_ITER = 200000;
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
                appender.startExcerpt();
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
