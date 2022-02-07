package net.openhft.chronicle.queue.benchmark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Simple command line parser
 * Use - to label an option
 * Each option is handled as a list, populated by the words following the option
 * eg: -opt one two three
 *     stores List(one, two, three) indexed by name "opt"
 */
public class Opts {

    private final static Map<String, List<String>> params = new HashMap<>();

    public static void parseArgs(String[] args) {

        List<String> options = null;
        for (int i = 0; i < args.length; i++) {
            final String a = args[i];

            if (a.charAt(0) == '-') {
                if (a.length() < 2) {
                    System.err.println("Error at argument " + a);
                    return;
                }

                options = new ArrayList<>();
                params.put(a.substring(1), options);
            }
            else if (options != null) {
                options.add(a);
            }
            else {
                System.err.println("Illegal parameter usage");
                return;
            }
        }
    }

    public static boolean empty() { return params.isEmpty(); }

    public static List<String> get(String name) {
        return params.get(name);
    }

    public static String getOrDefault(String name, String value) {
        if(params.containsKey(name))
            return params.get(name).get(0);

        return value;
    }

    public static boolean has(String name) {
        return params.containsKey(name);
    }
}
