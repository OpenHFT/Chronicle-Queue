package net.openhft.chronicle.queue.channel;

import net.openhft.chronicle.wire.utils.YamlTester;

import static org.junit.Assert.assertEquals;

public final class TestUtil {
    private TestUtil() {
    }

    public static void allowCommentsOutOfOrder(YamlTester yamlTester) {
        final String e = yamlTester.expected()
                .replaceAll("---\n", "");
        final String a = yamlTester.actual()
                .replaceAll("---\n", "");
        if (!e.equals(a))
            assertEquals(
                    yamlTester.expected(), yamlTester.actual());
    }
}
