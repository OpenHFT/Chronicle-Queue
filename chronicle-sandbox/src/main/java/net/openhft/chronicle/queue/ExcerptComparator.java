package net.openhft.chronicle.queue;

/**
 * For a binary search, provide a comparison of Excerpts
 */
public interface ExcerptComparator {
    /**
     * Given some criteria, deterime if the entry is -1 = below range, +1 = above range and 0 in range Can be used for
     * exact matches or a range of values.
     *
     * @param excerpt to check
     * @return -1 below, 0 = in range, +1 above range.
     */
    int compare(Excerpt excerpt);
}
