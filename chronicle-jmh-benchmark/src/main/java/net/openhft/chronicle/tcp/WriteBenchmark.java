/*
 * Copyright (c) 2005, 2014, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

package net.openhft.chronicle.tcp;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.tcp.network.SessionDetailsProvider;
import net.openhft.chronicle.tcp.network.SimpleSessionDetailsProvider;
import net.openhft.chronicle.tools.ChronicleTools;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.thread.LightPauser;
import net.openhft.lang.thread.Pauser;
import org.junit.rules.TemporaryFolder;
import org.openjdk.jmh.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static net.openhft.chronicle.ChronicleQueueBuilder.indexed;
import static net.openhft.lang.io.ByteBufferBytes.wrap;

@State(Scope.Benchmark)
public class WriteBenchmark {

    private static final Logger log = LoggerFactory.getLogger(WriteBenchmark.class);

    private final Bytes out = wrap(ByteBuffer.allocateDirect(64 << 20));

    private final SessionDetailsProvider sessionDetailsProvider = new SimpleSessionDetailsProvider();

    private final TemporaryFolder folder = new TemporaryFolder(new File(System.getProperty("java.io.tmpdir")));

    private final SourceTcp.SourceTcpHandler.IndexedSourceTcpHandler tcpHandler = new SourceTcp.SourceTcpHandler.IndexedSourceTcpHandler();

    private final Pauser pauser = new LightPauser(20000, 100000);

    private final int messages = 1000;

    private Chronicle source;

    private String getTmpDir(String name) {
        try {
            String mn = "benchmark";
            File path = mn == null
                    ? folder.newFolder(getClass().getSimpleName(), name)
                    : folder.newFolder(getClass().getSimpleName(), mn, name);

            log.debug("tmpdir: " + path);

            return path.getAbsolutePath();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    protected synchronized String getIndexedTestPath(String suffix) {
        final String path = getTmpDir(suffix);
        ChronicleTools.deleteOnExit(path);
        return path;
    }

    @Setup
    public void init() throws IOException {
        folder.create();
        final String basePathSource = getIndexedTestPath("source");
        source = indexed(basePathSource)
                .build();
        tcpHandler.tailer = source.createTailer();
        tcpHandler.pauser = pauser;

        final ExcerptAppender appender = source.createAppender();
        for (int i = 0; i < messages; i++) {
            appender.startExcerpt(1024);
            appender.writeLong(i);
            appender.finish();
        }

    }

    @TearDown
    public void teardown() throws IOException {
        if (source != null) {
            source.close();
        }
    }

    @Benchmark
    public int benchmarkWrite() {
        tcpHandler.tailer.toStart();
        out.clear();
        int i;
        for (i = 0; i < messages; i++) {
            tcpHandler.write(out, sessionDetailsProvider);
        }
        return i;
    }

}
