/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;

public class FileSystemDirectoryListingTest {
    @Rule
    public TemporaryFolder directory = new TemporaryFolder();

    private FileSystemDirectoryListing listing() {
        return new FileSystemDirectoryListing(directory.getRoot(),
                name -> Integer.parseInt(name.substring(0, 3)), () -> 1234);
    }

    private void file(String name) throws Exception {
        Files.createFile(new File(directory.getRoot(), name).toPath());
    }

    private void subdirectory(String name) throws Exception {
        Files.createDirectory(new File(directory.getRoot(), name).toPath());
    }

    private void assertEmpty(FileSystemDirectoryListing actual) {
        File missing = new File(directory.getRoot(), "absent-reference-directory");
        try (FileSystemDirectoryListing empty = new FileSystemDirectoryListing(missing,
                name -> { throw new AssertionError(name); }, () -> 1234)) {
            empty.refresh(true);
            assertEquals(empty.getMinCreatedCycle(), actual.getMinCreatedCycle());
            assertEquals(empty.getMaxCreatedCycle(), actual.getMaxCreatedCycle());
        }
    }

    @Test
    public void directoryAloneDoesNotCreateCycle() throws Exception {
        subdirectory("001.cq4");
        try (FileSystemDirectoryListing listing = listing()) {
            listing.refresh(true);
            assertEmpty(listing);
        }
    }

    @Test
    public void earlierDirectoryDoesNotLowerFirstCycle() throws Exception {
        subdirectory("001.cq4");
        file("002.cq4");
        try (FileSystemDirectoryListing listing = listing()) {
            listing.refresh(true);
            assertEquals(2, listing.getMinCreatedCycle());
            assertEquals(2, listing.getMaxCreatedCycle());
        }
    }

    @Test
    public void laterDirectoryDoesNotRaiseLastCycle() throws Exception {
        file("002.cq4");
        subdirectory("003.cq4");
        try (FileSystemDirectoryListing listing = listing()) {
            listing.refresh(true);
            assertEquals(2, listing.getMinCreatedCycle());
            assertEquals(2, listing.getMaxCreatedCycle());
        }
    }

    @Test
    public void metadataIsNotPassedToCycleParser() throws Exception {
        file("metadata.cq4t");
        file("002.cq4");
        try (FileSystemDirectoryListing listing = listing()) {
            listing.refresh(true);
            assertEquals(2, listing.getMinCreatedCycle());
            assertEquals(2, listing.getMaxCreatedCycle());
        }
    }

    @Test
    public void deletingExtremaResetsBounds() throws Exception {
        file("002.cq4");
        try (FileSystemDirectoryListing listing = listing()) {
            listing.refresh(true);
            assertEquals(2, listing.getMaxCreatedCycle());
            Files.delete(new File(directory.getRoot(), "002.cq4").toPath());
            listing.refresh(true);
            assertEmpty(listing);
        }
    }

    @Test
    public void missingDirectoryRefreshIsEmpty() {
        File missing = new File(directory.getRoot(), "missing");
        try (FileSystemDirectoryListing listing = new FileSystemDirectoryListing(missing,
                name -> { throw new AssertionError(name); }, () -> 1234)) {
            listing.refresh(true);
            assertEmpty(listing);
            assertEquals(1234, listing.lastRefreshTimeMS());
        }
    }
}
