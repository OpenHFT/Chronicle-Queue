/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single.stress.backwardstailer;

import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.rollcycles.LargeRollCycles;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@Fork(value = 1, warmups = 1)
@State(Scope.Benchmark)
public class BackwardsTailerToEndBoundaryJmhBenchmarkEndSpacingMinusOne {

    private BackwardsTailerJmhState state = new BackwardsTailerJmhState();

    @Setup(Level.Trial)
    public void setup() {
        RollCycle rollCycle = LargeRollCycles.LARGE_DAILY;
        state.setup(rollCycle.defaultIndexSpacing() - 1);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void run(Blackhole blackhole) {
        blackhole.consume(state.tailer().toEnd());
    }

    @TearDown(Level.Iteration)
    public void runComplete() {
        state.runComplete();
    }

    @TearDown(Level.Trial)
    public void complete() {
        state.complete();
    }
}
