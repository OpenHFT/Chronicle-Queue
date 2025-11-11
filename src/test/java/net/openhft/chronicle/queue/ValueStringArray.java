/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.values.Array;
import net.openhft.chronicle.values.MaxUtf8Length;
import net.openhft.chronicle.values.Values;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;

public class ValueStringArray extends SelfDescribingMarshallable {
    public interface CharSequenceArray {
        @Array(length = 3)
        void setCharSequenceWrapperAt(int index, CharSequenceWrapper value);

        CharSequenceWrapper getCharSequenceWrapperAt(int index);
    }

    public interface CharSequenceWrapper {
        void setCharSequence(@MaxUtf8Length(30) CharSequence charSequence);

        CharSequence getCharSequence();
    }

    private CharSequenceArray csArr = Values.newHeapInstance(CharSequenceArray.class);

    public CharSequenceArray getCsArr() {
        return csArr;
    }

    public void setCsArrItem(int index, String item) {
        this.csArr.getCharSequenceWrapperAt(index).setCharSequence(item);
    }
}
