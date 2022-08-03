/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
