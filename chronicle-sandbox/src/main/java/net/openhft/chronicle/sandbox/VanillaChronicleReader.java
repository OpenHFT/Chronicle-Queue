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

package net.openhft.chronicle.sandbox;

import net.openhft.lang.io.ByteBufferBytes;

import java.io.*;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TimeZone;

class VanillaChronicleReader {
    private static final int MILLI_PER_DAY = 24 * 60 * 60 * 1000;
    private final String baseDir;
    private final int cycleLength;
    private final SimpleDateFormat sdf;

    private VanillaChronicleReader(String baseDir) {
        this(baseDir, "yyyyMMdd", MILLI_PER_DAY);
    }

    private VanillaChronicleReader(String baseDir, String format, int cycleLength) {
        this.baseDir = baseDir;
        this.cycleLength = cycleLength;
        sdf = new SimpleDateFormat(format);
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
    }

    void dump(int cycle, Writer writer) throws IOException {
        Map<Long, ReaderFile> fileMap = new LinkedHashMap<Long, ReaderFile>(128, 0.7f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, ReaderFile> eldest) {
                boolean remove = size() > 64;
                if (remove) {
                    try {
                        eldest.getValue().close();
                    } catch (IOException e) {
                        // ignored
                    }
                }
                return remove;
            }
        };
        PrintWriter pw = writer instanceof PrintWriter ? (PrintWriter) writer : new PrintWriter(writer, true);
        File dir = new File(baseDir + "/" + sdf.format(new Date((long) cycle * cycleLength)));
        if (!dir.exists()) {
            pw.println("Directory " + dir + " does not exist.");
            return;
        }
        File[] files = dir.listFiles();
        int dataSize = -1;
        if (files != null) {
            for (File file : files) {
                if (file.getName().startsWith("data-")) {
                    long size = file.length();
                    if (size > 0 && size != dataSize) {
                        if (dataSize == -1) {
                            dataSize = (int) size;
                        } else {
                            pw.println("ERROR: Multiple data file sizes " + dataSize + " and " + size);
                            return;
                        }
                    }
                }
            }
        }
        if (dataSize <= 0) {
            pw.println("ERROR: Unable to determine data file size");
            return;
        }

        long count = 0;
        for (int i = 0; i < 10000; i++) {
            File file2 = new File(dir, "index-" + i);
            if (!file2.exists()) {
                pw.println(file2.getName() + " not present.");
                break;
            }
            FileChannel fc = new FileInputStream(file2).getChannel();
            ByteBufferBytes bbb = new ByteBufferBytes(fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size()).order(ByteOrder.nativeOrder()));

            while (bbb.remaining() > 0) {
                long index = bbb.readLong();
                if (index == 0) {
                    pw.println(count + ": unset");
                    return;
                }
                int threadId = (int) (index >>> 48);
                long offset = index & (-1L >>> -48);
                Long fileKey = index / dataSize;
                int fileOffset = (int) (index % dataSize);

                StringBuilder mesg = new StringBuilder();
                try {
                    ReaderFile readerFile = fileMap.get(fileKey);
                    if (readerFile == null) {
                        fileMap.put(fileKey, readerFile = new ReaderFile(new FileInputStream(new File(dir, "data-" + threadId + "-" + (offset / dataSize))).getChannel()));
                    }
                    MappedByteBuffer mbb = readerFile.mbb;
                    int length = ~mbb.getInt(fileOffset - 4);
                    for (int j = 0; j < length; j++) {
                        byte b = mbb.get(fileOffset + j);
                        if (b < ' ' || b >= 127) {
                            switch (b) {
                                case '\t':
                                    mesg.append("\\t");
                                    continue;
                                case '\r':
                                    mesg.append("\\r");
                                    continue;
                                case '\n':
                                    mesg.append("\\n");
                                    continue;
                            }
                            b = '.';
                        }
                        mesg.append((char) b);
                    }
                } catch (IOException e) {
                    mesg.append(e);
                }
                pw.println(count + ": " + threadId + " @" + offset + " | " + mesg);
                count++;
            }
            fc.close();
        }
        for (ReaderFile file : fileMap.values()) {
            file.close();
        }
        fileMap.clear();
        pw.flush();
    }

    public static void main(String... args) throws IOException {
        int cycle = (int) (System.currentTimeMillis() / MILLI_PER_DAY);
        Writer out = new OutputStreamWriter(System.out);
        new VanillaChronicleReader(args[0]).dump(cycle, out);
        System.out.println("Done.");
    }

    static class ReaderFile implements Closeable {
        final FileChannel channel;
        final MappedByteBuffer mbb;

        ReaderFile(FileChannel channel) throws IOException {
            this.channel = channel;
            mbb = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
            mbb.order(ByteOrder.nativeOrder());
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }
}
