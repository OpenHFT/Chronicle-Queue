/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.onoes.ExceptionKey;
import net.openhft.chronicle.core.onoes.LogLevel;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import net.openhft.chronicle.testframework.exception.ExceptionTracker;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class NormaliseEOFsTest extends QueueTestCommon {

    private static final String LOG_LEVEL_PROPERTY = "org.slf4j.simpleLogger.log." + StoreAppender.class.getName();
    private static final File QUEUE_PATH = Paths.get(OS.getTarget(), "normaliseEOFsTest").toFile();
    private Map<ExceptionKey, Integer> exceptionMap;

    @Before
    public void setLogLevelProperty() {
        System.setProperty(LOG_LEVEL_PROPERTY, "debug");
    }

    @Before
    public void clearDataFromPreviousRun() {
        IOTools.deleteDirWithFilesOrThrow(QUEUE_PATH);
    }

    @Before
    @Override
    public void recordExceptions() {
        super.recordExceptions();
        exceptionMap = Jvm.recordExceptions(true);
        exceptionTracker = ExceptionTracker.create(ExceptionKey::message, ExceptionKey::throwable, Jvm::resetExceptionHandlers, exceptionMap,
                (key) -> key.level != LogLevel.DEBUG && key.level != LogLevel.PERF,
                (key) -> key.level() + " " + key.clazz().getSimpleName() + " " + key.message());
        ignoreException(ex -> true, "Ignore everything");
    }

    @After
    public void clearLogLevelProperty() {
        System.clearProperty(LOG_LEVEL_PROPERTY);
    }

    @After
    public void cleanupQueueData() {
        BackgroundResourceReleaser.releasePendingResources();
        IOTools.deleteDirWithFilesOrThrow(QUEUE_PATH);
    }

    @Test
    public void normaliseShouldResumeFromPreviousNormalisation() {
        SetTimeProvider setTimeProvider = new SetTimeProvider();
        try (final SingleChronicleQueue queue = createQueue(setTimeProvider);
             final ExcerptAppender excerptAppender = queue.createAppender()) {
            for (int i = 0; i < 5; i++) {
                createNewRollCycles(excerptAppender, setTimeProvider);
                excerptAppender.normaliseEOFs();
            }
            final Pattern logPattern = Pattern.compile("Normalising from cycle (\\d+)");
            // Note: lock the exceptionMap to avoid a concurrent modification exceptions leading to flakiness under load
            final List<Integer> startIndices;
            synchronized (exceptionMap) {
                startIndices = exceptionMap.keySet().stream()
                        .map(exceptionKey -> logPattern.matcher(exceptionKey.message))
                        .filter(Matcher::matches)
                        .map(matcher -> Integer.parseInt(matcher.group(1)))
                        .collect(Collectors.toList());
            }

            // There is at least 5 calls to normaliseEOF and the start index increases each time
            assertTrue(startIndices.size() >= 5);
            int lastStartIndex = Integer.MIN_VALUE;
            for (final int startIndex : startIndices) {
                assertTrue(startIndex > lastStartIndex);
                lastStartIndex = startIndex;
            }
        }
    }

    private void createNewRollCycles(ExcerptAppender appender, SetTimeProvider timeProvider) {
        for (int i = 0; i < 10; i++) {
            timeProvider.advanceMillis(3_000);
            try (final DocumentContext documentContext = appender.writingDocument()) {
                documentContext.wire().write("aaa").text("bbb");
            }
        }
    }

    private SingleChronicleQueue createQueue(TimeProvider setTimeProvider) {
        return SingleChronicleQueueBuilder.binary(QUEUE_PATH).timeProvider(setTimeProvider).rollCycle(TestRollCycles.TEST_SECONDLY).build();
    }
}
