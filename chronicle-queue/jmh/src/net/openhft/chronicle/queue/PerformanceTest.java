/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireType;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
public class PerformanceTest {

    public static final WireType WIRE_TYPE = WireType.BINARY;
    public static final int MESSAGE_SIZE_IN_BYTES = 200;

    /*
     * And, check the benchmark went fine afterwards:
     */
    public static void main(String[] args) throws Exception {
        if (Jvm.isDebug()) {
            PerformanceTest main = new PerformanceTest();
            main.setUp();
            for (Method m : PerformanceTest.class.getMethods()) {
                if (m.getAnnotation(Benchmark.class) != null) {
                    for (int i = 0; i < 100; i++) {
                        for (int j = 0; j < 100; j++) {
                            m.invoke(main);

                        }
                        System.out.println("");
                    }
                }
            }
            main.tearDown();
        } else {
            int time = Boolean.getBoolean("longTest") ? 30 : 2;
            System.out.println("measurementTime: " + time + " secs");
            Options opt = new OptionsBuilder()
                    .include(PerformanceTest.class.getSimpleName())
                    .warmupIterations(5)
//                .measurementIterations(5)
                    .forks(1)
                    .mode(Mode.SampleTime)
                    .measurementTime(TimeValue.seconds(time))
                    .timeUnit(TimeUnit.NANOSECONDS)
                    .build();

            new Runner(opt).run();
        }
    }

    Bytes<ByteBuffer> buffer;
    ExcerptAppender appender;
    File file;

    @Setup
    public void setUp() throws Exception {
        file = File.createTempFile("chronicle.", "q");
        file.deleteOnExit();

        final char[] chars = new char[MESSAGE_SIZE_IN_BYTES];
        Arrays.fill(chars, 'X');
        final String str200 = new String(chars);

        buffer = Bytes.elasticByteBuffer();
        buffer.write(str200);
        final ChronicleQueue chronicle = new SingleChronicleQueueBuilder(
                Files.createTempDirectory("chronicle" + "-").toFile())
                .wireType(WIRE_TYPE)
                .build();

        appender = chronicle.createAppender();


    }

    @TearDown
    public void tearDown() throws IOException {
        file.delete();
        System.out.println("closed");
    }

    StringBuilder sb = new StringBuilder();

    @Benchmark
    public String testAppend200ByteString() throws IOException {
        final long l = appender.writeBytes(buffer);
        sb.setLength(0);
        sb.append(l);
        return sb.toString();
    }


}
