package net.openhft.chronicle.queue.impl.single.namedtailer;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;

public class IndexUpdaterFactory {

    public static IndexUpdater createIndexUpdater(String tailerName, SingleChronicleQueue queue) {
        if (tailerName == null) {
            return null;
        } else if (tailerName.startsWith(SingleChronicleQueue.REPLICATED_NAMED_TAILER_PREFIX)) {
            return new VersionedIndexUpdater(
                    queue.indexForId(tailerName),
                    queue.indexVersionForId(tailerName)
            );
        } else {
            return new StandardIndexUpdater(queue.indexForId(tailerName));
        }
    }
}
