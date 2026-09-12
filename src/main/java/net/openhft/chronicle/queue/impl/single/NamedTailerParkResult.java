/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

/**
 * Operational outcome of an attempt to park a stopped named tailer whose owner has closed it.
 * {@link #PARKED} means the persisted position was reset, {@link #NOT_FOUND} means no persisted
 * registration exists, and {@link #REFUSED_REPLICATED} means Queue left version-coordinated
 * replication state unchanged. Invalid caller input is reported by
 * {@link SingleChronicleQueue#parkNamedTailer(String)} as an exception rather than an outcome.
 */
//! SingleChronicleQueueNamedTailerMetadataTest#parkNamedTailerResetsExistingNonReplicatedTailer,
//! #parkNamedTailerDoesNotCreateMissingTailer and #replicatedNamedTailersCannotBeParked require
//! distinct outcomes so maintenance can distinguish mutation, absence and a safety refusal.
public enum NamedTailerParkResult {
    /** The named tailer existed and its persisted index was reset to zero. */
    PARKED,
    /** No persisted named tailer exists with the supplied name. */
    NOT_FOUND,
    /** The named tailer is replicated and cannot safely be parked locally. */
    REFUSED_REPLICATED
}
