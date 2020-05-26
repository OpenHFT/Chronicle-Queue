/*
 * Copyright 2016-2020 Chronicle Software
 *
 * https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue;

import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;

import static org.junit.Assert.assertNull;

/**
 * Idea borrowed from Netty - https://github.com/netty/netty - microbench/src/main/java/io/netty/microbench/util/AbstractMicrobenchmarkBase.java
 */
public class ChronicleQueueMicrobench {

    protected static final int DEFAULT_WARMUP_ITERATIONS = 10;
    protected static final int DEFAULT_MEASURE_ITERATIONS = 10;

    private static final String[] EMPTY_JVM_ARGS = {};

    private static final String[] BASE_JVM_ARGS = {
            "-server",
            "-dsa",
            "-da",
            "-ea:net.openhft...",
            "-XX:+AggressiveOpts",
            "-XX:+UseBiasedLocking",
            "-XX:+UseFastAccessorMethods",
            "-XX:+OptimizeStringConcat",
            "-XX:+HeapDumpOnOutOfMemoryError"
    };

    // *************************************************************************
    //
    // *************************************************************************

    public static void handleUnexpectedException(Throwable t) {
        assertNull(t);
    }

    public static void main(String[] args) throws RunnerException {
        new Runner(new ChronicleQueueMicrobench().newOptionsBuilder().build()).run();
    }

    private ChainedOptionsBuilder newOptionsBuilder() {
        String className = getClass().getSimpleName();

        final ChainedOptionsBuilder runnerOptions = new OptionsBuilder()
                .include(".*" + className + ".*")
                .jvmArgs(BASE_JVM_ARGS)
                .jvmArgsAppend(jvmArgs()
                );

        if (getWarmupIterations() > 0) {
            runnerOptions.warmupIterations(getWarmupIterations());
        }

        if (getMeasureIterations() > 0) {
            runnerOptions.measurementIterations(getMeasureIterations());
        }

        if (null != getReportDir()) {
            String filePath = getReportDir() + className + ".json";
            File file = new File(filePath);
            if (file.exists()) {
                file.delete();
            } else {
                file.getParentFile().mkdirs();
//                file.createNewFile();
            }

            runnerOptions.resultFormat(ResultFormatType.JSON);
            runnerOptions.result(filePath);
        }

        return runnerOptions;
    }

    @NotNull
    private String[] jvmArgs() {
        return EMPTY_JVM_ARGS;
    }

    private int getWarmupIterations() {
        return Integer.getInteger("warmupIterations", -1);
    }

    private int getMeasureIterations() {
        return Integer.getInteger("measureIterations", -1);
    }

    // *************************************************************************
    //
    // *************************************************************************

    private String getReportDir() {
        return System.getProperty("perfReportDir");
    }
}
