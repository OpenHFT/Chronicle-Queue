package net.openhft.chronicle.tcp;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

@State(Scope.Benchmark)
public class ComparisonBenchmark {

    private int i = 0;

    private int j = Character.MAX_VALUE;

    @Benchmark
    public int bitShift() {
        i++;
        j--;
        return i << 1 > j ? i : j;
    }

    @Benchmark
    public int multiply() {
        i++;
        j--;
        return i > j * 0.5 ? i : j;
    }

}
