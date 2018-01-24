package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.impl.TableStore;

import java.util.Arrays;
import java.util.function.Function;

final class ReferenceTracker {
    private static final int CACHE_SIZE = 64;
    private static final int INDEX_MASK = CACHE_SIZE - 1;
    private final TableStore backingStore;
    private final ReverseCharSequenceIntegerEncoder encoder = new ReverseCharSequenceIntegerEncoder();
    private final CachedLongValue[] cache = new CachedLongValue[CACHE_SIZE];
    private final Function<TableStore, LongValue> safeAcquireFunction = this::safelyGetLongValue;

    ReferenceTracker(final TableStore backingStore) {
        this.backingStore = backingStore;
        Arrays.setAll(cache, i -> new CachedLongValue());
    }

    synchronized void acquired(final int cycle) {
        acquireLongValue(cycle).addAtomicValue(1);
    }

    synchronized void released(final int cycle) {
        acquireLongValue(cycle).addAtomicValue(-1);
    }

    private LongValue acquireLongValue(final int cycle) {
        final CachedLongValue cachedValue = cache[mask(cycle)];
        if (cachedValue.cycle != cycle) {
            encoder.encode(cycle);
            cachedValue.cycle = cycle;
            cachedValue.value = backingStore.doWithExclusiveLock(safeAcquireFunction);
            if (cachedValue.value.getVolatileValue() == Long.MIN_VALUE) {
                cachedValue.value.compareAndSwapValue(Long.MIN_VALUE, 0);
            }
        }
        return cachedValue.value;
    }

    private LongValue safelyGetLongValue(final TableStore tableStore) {
        return tableStore.acquireValueFor(encoder);
    }

    private static int mask(final int cycle) {
        return cycle & INDEX_MASK;
    }

    private static final class CachedLongValue {
        private int cycle = -1;
        private LongValue value;
    }

    static final class ReverseCharSequenceIntegerEncoder implements CharSequence {
        private final char[] data = new char[Integer.toString(Integer.MAX_VALUE).length()];
        private int length;
        private int indexOffset;

        void encode(int value) {
            validate(value);

            length = 0;
            while (value != 0) {
                data[length++] = (char) ('0' + (value % 10));
                value /= 10;
            }

            handleZero();
            indexOffset = length - 1;
        }

        @Override
        public int length() {
            return length;
        }

        @Override
        public char charAt(final int index) {
            return data[indexOffset - index];
        }

        @Override
        public CharSequence subSequence(final int start, final int end) {
            throw new UnsupportedOperationException();
        }

        private void handleZero() {
            if (length == 0) {
                length = 1;
                data[0] = '0';
            }
        }

        private static void validate(final int value) {
            if (value < 0) {
                throw new UnsupportedOperationException();
            }
        }
    }
}