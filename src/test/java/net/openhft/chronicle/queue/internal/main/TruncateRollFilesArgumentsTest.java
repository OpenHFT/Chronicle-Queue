/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.main;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import static org.junit.Assert.*;

public class TruncateRollFilesArgumentsTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private int run(String... args) throws Exception {
        try (PrintStream sink = new PrintStream(new ByteArrayOutputStream(), false, "UTF-8")) {
            return InternalTruncateRollFilesMain.run(args, sink, sink);
        }
    }

    @Test public void optionsAcceptEitherOrderAndZeroAge() throws Exception {
        String root = temporary.newFolder().toString();
        assertEquals(0, run(root, "--offline", "--min-age-seconds=0"));
        assertEquals(0, run(root, "--min-age-seconds=600", "--offline"));
        assertEquals(0, run(root));
    }

    @Test public void invalidArgumentsHaveUsageExitCode() throws Exception {
        String root = temporary.newFolder().toString();
        assertEquals(2, run());
        for (String option : new String[]{"--unknown", "--min-age-seconds=", "--min-age-seconds=+0",
                "--min-age-seconds=-1", "--min-age-seconds=1.0", "--min-age-seconds=٠",
                "--min-age-seconds=9223372036854776"})
            assertEquals(option, 2, run(root, option));
        assertEquals(2, run(root, "--offline", "--offline"));
        assertEquals(2, run(root, "--min-age-seconds=0", "--min-age-seconds=1"));
    }

    @Test public void unsafeOrMissingRootHasIncompleteExitCode() throws Exception {
        Path root = temporary.newFolder().toPath();
        assertEquals(1, run(root.resolve("missing").toString()));
        Path alias = root.resolve("alias");
        java.nio.file.Files.createSymbolicLink(alias, root);
        assertEquals(1, run(alias.toString()));
    }
}
