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

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ExcerptCommon;
import net.openhft.chronicle.VanillaChronicle;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.IByteBufferBytes;
import net.openhft.lang.io.IOTools;
import net.openhft.lang.model.constraints.NotNull;
import org.slf4j.Logger;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
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

    public static void logIOException(Logger logger, String message, IOException e) {
        if (e instanceof EOFException) {
            logger.trace(message, e);

        } else {
            logger.warn(message, e);
        }
    }

    public static String asString(ByteBuffer bb) {
        IByteBufferBytes wrap = ByteBufferBytes.wrap(bb);
        wrap.position(bb.position());
        wrap.limit(bb.limit());
        return wrap.toDebugString();
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

    public static void warmup() {
        //noinspection UnusedDeclaration needed to laod class.
        boolean done = ChronicleWarmup.Indexed.DONE;
    }

    public static void checkCount(final @NotNull Chronicle chronicle, int min, int max) {
        if (chronicle instanceof VanillaChronicle) {
            ((VanillaChronicle) chronicle).checkCounts(min, max);
        }
    }

    public static ClassLoader getSystemClassLoader() {
        if (System.getSecurityManager() == null) {
            return ClassLoader.getSystemClassLoader();

        } else {
            return AccessController.doPrivileged(new PrivilegedAction<ClassLoader>() {
                @Override
                public ClassLoader run() {
                    return ClassLoader.getSystemClassLoader();
                }
            });
        }
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
}