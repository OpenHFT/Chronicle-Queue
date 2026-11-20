/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.table;

import net.openhft.chronicle.core.io.IORuntimeException;

import java.io.File;

//! Give read-only fallback a provenance-bearing signal emitted only while the metadata file is opened or awaited.
//! SingleChronicleQueueCorruptMetadataTest#nestedFileNotFoundFromMetadataDoesNotActivateReadonlyFallback and
//! #constructorAvailabilitySignalDoesNotActivateReadonlyFallback prevent decoder causes from masquerading as availability.
//! The type is public so callers can distinguish availability, but its package-owned constructors reserve fallback authority
//! to Queue's file-opening code.
//! SingleChronicleQueueCorruptMetadataTest#metadataAccessorUnavailabilityDoesNotActivateReadonlyFallback proves that
//! receiving this exact type later in queue setup does not broaden that authority.
/**
 * Indicates that a table-store file could not be opened or had not yet published enough data to be mapped.
 *
 * <p>This is distinct from {@link CorruptTableStoreException}: absence or an access failure may permit a queue to
 * fall back to caller-supplied metadata in a synthetic read-only store, whereas inconsistent content never does.</p>
 *
 * <p>Construction is package-restricted so only Queue's table-store opening code can originate this signal.
 * Callers may nevertheless catch the public type and distinguish it from persisted-content corruption.</p>
 */
public final class TableStoreUnavailableException extends IORuntimeException {
    private static final long serialVersionUID = 0L;

    /**
     * @param file    the unavailable table-store file
     * @param message the bounded availability reason
     */
    TableStoreUnavailableException(File file, String message) {
        super(message + ": " + file);
    }

    /**
     * @param file    the unavailable table-store file
     * @param message the bounded availability reason
     * @param cause   the failure observed at the file-opening boundary
     */
    TableStoreUnavailableException(File file, String message, Throwable cause) {
        super(message + ": " + file, cause);
    }
}
