package net.openhft.chronicle.queue.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Container for a list of cpus passed on the command line
 * eg: -cpus 1 2 5 7
 * stores 1 2 5 7 in the Cpus instance
 */
public class Cpus {

    private static ArrayList<Integer> CPUS = new ArrayList<>();
    private static AtomicInteger idx = new AtomicInteger();

    /**
     * Initialise - requires that Opts.parseArgs is called first to pick up command line options
     */
    public static void initialise() {

        List<String> cpus = Opts.get("cpus");
        StringBuilder str = new StringBuilder();
        if(cpus != null) {
            for(int c=0; c < cpus.size(); ++c) {
                if(c!=0) str.append(',');
                int cpu = Integer.parseInt(cpus.get(c));
                CPUS.add(cpu);
                str.append(cpu);
            }
            System.out.println("Reserved CPUs = " + str.toString());
        }
    }

    /**
     * Get next reserved CPU
     * @return
     */
    public static int next() {
        return get(idx.getAndIncrement() % CPUS.size());
    }

    /**
     * Get number of reserved CPUs
     * @return
     */
    public static int count() {
        return CPUS.size();
    }

    /**
     * Request nth cpu in the list
     * @param n
     * @return
     */
    public static int get(int n) {
        if(CPUS.isEmpty())
            throw new IllegalArgumentException("No reserved CPUs defined. Set using -cpus command line option");

        if(n >= CPUS.size())
            throw new IllegalArgumentException("CPU index " + n + " too large. Valid range is [0," + CPUS.size() + ")");

        return CPUS.get(n);
    }
}

