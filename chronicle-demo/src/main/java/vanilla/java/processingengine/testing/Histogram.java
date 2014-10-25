/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package vanilla.java.processingengine.testing;

import net.openhft.lang.model.constraints.NotNull;

/**
 * @author peter.lawrey
 */
class Histogram {
    @NotNull
    private final int[] count;
    private final int factor;
    private int underflow;
    private int overflow;
    private long total = 0;

    public Histogram(int counts, int factor) {
        this.count = new int[counts + 1];
        this.factor = factor;
    }

    public void sample(long n) {
        long bucket = (n + factor / 2) / factor;
        if (bucket < 0)
            underflow++;
        else if (bucket >= count.length)
            overflow++;
        else
            count[((int) bucket)]++;
        total++;
    }

    public int percentile(double d) {
        long num = total - (long) (d * total);
        if (num <= overflow)
            return Integer.MAX_VALUE;
        for (int i = count.length - 1; i > 0; i--)
            if ((num -= count[i]) <= 0)
                return i * factor;
        return 0;
    }

    public long underflow() {
        return underflow;
    }

    public long overflow() {
        return overflow;
    }
}
