/*
 * Copyright 2016-2025 chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue.impl.single.stress.backwardstailer;

import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.rollcycles.LargeRollCycles;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@Fork(value = 1, warmups = 1)
@State(Scope.Benchmark)
public class BackwardsTailerToEndBoundaryJmhBenchmark {

    private BackwardsTailerJmhState state = new BackwardsTailerJmhState();

    @Setup(Level.Trial)
    public void setup() {
        RollCycle rollCycle = LargeRollCycles.LARGE_DAILY;
        state.setup(rollCycle.defaultIndexCount() * rollCycle.defaultIndexSpacing());
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
