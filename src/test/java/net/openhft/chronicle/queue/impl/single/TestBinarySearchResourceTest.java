/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.wire.Wire;
import org.junit.Test;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import static org.junit.Assert.*;

public class TestBinarySearchResourceTest extends QueueTestCommon {
    @Test public void releasesKeysAfterSuccessfulSearch() { checkRelease(2, "normal"); }
    @Test public void releasesKeyAfterMissingSearch() { checkRelease(0, "normal"); }
    @Test public void releasesKeyAfterSearchFailure() { checkRelease(2, "search"); }
    @Test public void releasesKeyAfterAssertionFailure() { checkRelease(2, "assertion"); }
    @Test public void releasesMissingKeyAfterSearchFailure() { checkRelease(0, "search"); }
    @Test public void releasesMissingKeyAfterAssertionFailure() { checkRelease(0, "assertion"); }

    private void checkRelease(int verify, String failureMode) {
        final List<Wire> keys = new ArrayList<>();
        final RuntimeException searchFailure = new IllegalStateException("injected search failure");
        TestBinarySearch fixture = new TestBinarySearch(2, verify, TestBinarySearch.EmptyCyclesStrategy.NO_EMPTY_CYCLES) {
            @Override protected java.io.File getTmpDir() {
                return TestBinarySearchResourceTest.this.getTmpDir();
            }
            @Override Wire toWire(int key) {
                Wire wire = super.toWire(key);
                keys.add(wire);
                return wire;
            }
            @Override Comparator<Wire> comparator() {
                if ("search".equals(failureMode))
                    return (left, right) -> { throw searchFailure; };
                if ("assertion".equals(failureMode))
                    return (left, right) -> 0;
                return super.comparator();
            }
        };
        try {
            Throwable caught = null;
            try {
                fixture.testBinarySearch();
            } catch (RuntimeException | AssertionError failure) {
                caught = failure;
            }
            if ("normal".equals(failureMode)) {
                assertNull(caught);
                assertEquals("observed keys including missing key", verify + 1, keys.size());
            } else if ("search".equals(failureMode)) {
                assertSame("original search failure", searchFailure, caught);
            } else {
                assertTrue("original search assertion", caught instanceof AssertionError);
                assertTrue("assertion came from search result", caught.getMessage().contains(
                        verify == 0 ? "Should not find non-existent" : "Failed looking for item"));
            }
            assertFalse("a key was acquired", keys.isEmpty());
            for (Wire key : keys)
                assertEquals("search key bytes must be released on every exit", 0, key.bytes().refCount());
        } finally {
            // Keep a failing mutation from leaking into the enclosing fixture after observing it.
            for (Wire key : keys)
                if (key.bytes().refCount() > 0)
                    key.bytes().releaseLast();
        }
    }
}
