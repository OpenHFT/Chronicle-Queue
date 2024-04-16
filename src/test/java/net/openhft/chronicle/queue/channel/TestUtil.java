package net.openhft.chronicle.queue.channel;

import net.openhft.chronicle.wire.utils.YamlTester;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

@SuppressWarnings("deprecated")
public final class TestUtil {

    public static void allowCommentsOutOfOrder(YamlTester yamlTester) {

        final String e = commentsFirst(yamlTester.expected());
        final String a = commentsFirst(yamlTester.actual());
        if (!e.equals(a))
            assertEquals(
                    yamlTester.expected(), yamlTester.actual());
    }

    static String commentsFirst(String s) {
        return Stream.of(s.split("\\n"))
                .filter(l -> !l.equals("---"))
                .sorted((s1, t) -> s1.startsWith("# ")
                        ? t.startsWith("# ") ? 0 : -1
                        : +1)
                .collect(Collectors.joining("\n"));
    }

    @Test
    public void sorted() {
        assertEquals("" +
                        "# first message\n" +
                        "# second message\n" +
                        "say: Hello World\n" +
                        "...\n" +
                        "say: \"Bye, now\"\n" +
                        "...",
                commentsFirst("" +
                        "# first message\n" +
                        "---\n" +
                        "say: Hello World\n" +
                        "...\n" +
                        "---\n" +
                        "# second message\n" +
                        "say: \"Bye, now\"\n" +
                        "..."));
    }
}
