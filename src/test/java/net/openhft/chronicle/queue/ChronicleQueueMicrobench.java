/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.openhft.chronicle.queue;

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

    protected static final String[] EMPTY_JVM_ARGS = {};

    protected static final String[] BASE_JVM_ARGS = {
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

    protected ChainedOptionsBuilder newOptionsBuilder() {
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

    protected String[] jvmArgs() {
        return EMPTY_JVM_ARGS;
    }

    protected int getWarmupIterations() {
        return Integer.getInteger("warmupIterations", -1);
    }

    protected int getMeasureIterations() {
        return Integer.getInteger("measureIterations", -1);
    }

    protected String getReportDir() {
        return System.getProperty("perfReportDir");
    }

    public static void handleUnexpectedException(Throwable t) {
        assertNull(t);
    }

    // *************************************************************************
    //
    // *************************************************************************

    public static void main(String[] args) throws RunnerException {
        new Runner(new ChronicleQueueMicrobench().newOptionsBuilder().build()).run();
    }
}
