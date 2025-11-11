/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
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
