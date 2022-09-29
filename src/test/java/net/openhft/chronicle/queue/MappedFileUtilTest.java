package net.openhft.chronicle.queue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class MappedFileUtilTest {

    private final String procMapsLine;
    private final String expectedPath;
    private final String expectedAddress;

    @Parameterized.Parameters(name = "line={0} should resolve path={1} and address={2}")
    public static List<Object[]> exampleLines() {
        return Arrays.asList(
                new Object[]{"7f3b9ee38000-7f3b9ee3b000 r--p 00000000 103:03 3147723                   /usr/lib/x86_64-linux-gnu/libgcc_s.so.1", "/usr/lib/x86_64-linux-gnu/libgcc_s.so.1", "7f3b9ee38000-7f3b9ee3b000"},
                new Object[]{"d0000000-e4b00000 rw-p 00000000 00:00 0", "", "d0000000-e4b00000"},
                new Object[]{"100240000-140000000 ---p 00000000 00:00 0", "", "100240000-140000000"},
                new Object[]{"564609d75000-564609d76000 r--p 00000000 fe:02 15994495                   /usr/lib/jvm/java-8-openjdk/jre/bin/java", "/usr/lib/jvm/java-8-openjdk/jre/bin/java", "564609d75000-564609d76000"},
                new Object[]{"56460ac26000-56460ac47000 rw-p 00000000 00:00 0                          [heap]", "[heap]", "56460ac26000-56460ac47000"},
                new Object[]{"7f22b44b3000-7f22b44b4000 r--s 00001000 fe:02 11535264                   /opt/intellij-idea-ultimate-edition/plugins/maven/lib/maven-event-listener.jar", "/opt/intellij-idea-ultimate-edition/plugins/maven/lib/maven-event-listener.jar", "7f22b44b3000-7f22b44b4000"},
                new Object[]{"ffffffffff600000-ffffffffff601000 --xp 00000000 00:00 0                  [vsyscall]", "[vsyscall]", "ffffffffff600000-ffffffffff601000"}
        );
    }

    public MappedFileUtilTest(String procMapsLine, String expectedPath, String expectedAddress) {
        this.procMapsLine = procMapsLine;
        this.expectedPath = expectedPath;
        this.expectedAddress = expectedAddress;
    }

    @Test
    public void shouldParseExampleLine() {
        final Matcher matcher = MappedFileUtil.parseMapsLine(procMapsLine);
        assertTrue(matcher.matches());
        assertEquals(expectedPath, MappedFileUtil.getPath(matcher));
        assertEquals(expectedAddress, MappedFileUtil.getAddress(matcher));
    }
}
