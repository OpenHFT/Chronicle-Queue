/*
 * Copyright 2016-2020 chronicle.software
 *
 * https://chronicle.software
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
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.QueueTestCommon;
import org.jetbrains.annotations.NotNull;
import org.junit.Assume;
import org.junit.Test;

import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

import static org.junit.Assert.assertEquals;

public class PretoucherStateTest extends QueueTestCommon {
    @Test
    public void pretouch() {

        // todo remove see https://github.com/OpenHFT/Chronicle-Queue/issues/837
        Assume.assumeTrue(!Jvm.isMacArm());

        long[] pos = {0};
        final StringBuilder record = new StringBuilder();
        PretoucherState ps = new DummyPretoucherState(() -> pos[0] += 4096, 64 << 10, record, () -> false);
        ps.pretouch(null);
        ps.pretouch(null);
        assertEquals("debug none - Reset pretoucher to pos 4096 as the underlying MappedBytes changed.\n" +
                "touchPage 1 til 17 count 17\n" +
                "debug pretouch for only 0 of 17 min: 0 MB.\n" +
                "debug none: Advanced 4 KB, avg 4 KB between pretouch() and 4 KB while mapping of 0 KB \n", record.toString());
    }

    @Test
    public void pretouchLeap() {

        // todo remove see https://github.com/OpenHFT/Chronicle-Queue/issues/837
        Assume.assumeTrue(!Jvm.isMacArm());

        long[] pos = {1 << 20};
        final StringBuilder record = new StringBuilder();
        PretoucherState ps = new DummyPretoucherState(() -> pos[0] += 4 << 10, 16 << 10, record, () -> true);
        ps.pretouch(null); // reset();
        for (int i = 0; i < 8; i++) {
            ps.pretouch(null);
            pos[0] += 56 << 10; // 56 + 4 + 4 = 64 in total.
        }
        ps.pretouch(null);
        assertEquals("debug none - Reset pretoucher to pos 1052672 as the underlying MappedBytes changed.\n" +
                "touchPage 257 til 261 count 5\n" +
                "debug none: Advanced 4 KB, avg 4 KB between pretouch() and 4 KB while mapping of 20 KB \n" +
                "touchPage 262 til 292 count 31\n" +
                "debug none: Advanced 64 KB, avg 19 KB between pretouch() and 4 KB while mapping of 124 KB \n" +
                "touchPage 293 til 320 count 28\n" +
                "debug none: Advanced 64 KB, avg 30 KB between pretouch() and 4 KB while mapping of 112 KB \n" +
                "touchPage 321 til 344 count 24\n" +
                "debug none: Advanced 64 KB, avg 38 KB between pretouch() and 4 KB while mapping of 96 KB \n" +
                "touchPage 345 til 367 count 23\n" +
                "debug none: Advanced 64 KB, avg 45 KB between pretouch() and 4 KB while mapping of 92 KB \n" +
                "touchPage 368 til 387 count 20\n" +
                "debug none: Advanced 64 KB, avg 49 KB between pretouch() and 4 KB while mapping of 80 KB \n" +
                "touchPage 388 til 407 count 20\n" +
                "debug none: Advanced 64 KB, avg 53 KB between pretouch() and 4 KB while mapping of 80 KB \n" +
                "touchPage 408 til 425 count 18\n" +
                "debug none: Advanced 64 KB, avg 55 KB between pretouch() and 4 KB while mapping of 72 KB \n" +
                "touchPage 426 til 443 count 18\n" +
                "debug none: Advanced 64 KB, avg 57 KB between pretouch() and 4 KB while mapping of 72 KB \n", record.toString());
    }

    @Test
    public void pretouchLongBreak() {
        // todo remove see https://github.com/OpenHFT/Chronicle-Queue/issues/837
        Assume.assumeTrue(!Jvm.isMacArm());
        
        long[] pos = {0};
        final StringBuilder record = new StringBuilder();
        PretoucherState ps = new DummyPretoucherState(() -> pos[0] += 2 * 1024, 16 << 10, record, () -> true);
        for (int i = 0; i <= 10; i++) {
            record.append("pos: ").append(pos[0]).append(", i:").append(i).append("\n");
            ps.pretouch(null);
        }
        assertEquals("pos: 0, i:0\n" +
                "debug none - Reset pretoucher to pos 2048 as the underlying MappedBytes changed.\n" +
                "pos: 2048, i:1\n" +
                "touchPage 0 til 4 count 5\n" +
                "debug none: Advanced 2 KB, avg 3 KB between pretouch() and 2 KB while mapping of 20 KB \n" +
                "pos: 6144, i:2\n" +
                "touchPage 5 til 5 count 1\n" +
                "debug none: Advanced 4 KB, avg 3 KB between pretouch() and 2 KB while mapping of 4 KB \n" +
                "pos: 10240, i:3\n" +
                "touchPage 6 til 6 count 1\n" +
                "debug none: Advanced 4 KB, avg 3 KB between pretouch() and 2 KB while mapping of 4 KB \n" +
                "pos: 14336, i:4\n" +
                "touchPage 7 til 7 count 1\n" +
                "debug none: Advanced 4 KB, avg 3 KB between pretouch() and 2 KB while mapping of 4 KB \n" +
                "pos: 18432, i:5\n" +
                "touchPage 8 til 8 count 1\n" +
                "debug none: Advanced 4 KB, avg 3 KB between pretouch() and 2 KB while mapping of 4 KB \n" +
                "pos: 22528, i:6\n" +
                "touchPage 9 til 9 count 1\n" +
                "debug none: Advanced 4 KB, avg 3 KB between pretouch() and 2 KB while mapping of 4 KB \n" +
                "pos: 26624, i:7\n" +
                "touchPage 10 til 10 count 1\n" +
                "debug none: Advanced 4 KB, avg 3 KB between pretouch() and 2 KB while mapping of 4 KB \n" +
                "pos: 30720, i:8\n" +
                "touchPage 11 til 11 count 1\n" +
                "debug none: Advanced 4 KB, avg 3 KB between pretouch() and 2 KB while mapping of 4 KB \n" +
                "pos: 34816, i:9\n" +
                "touchPage 12 til 12 count 1\n" +
                "debug none: Advanced 4 KB, avg 3 KB between pretouch() and 2 KB while mapping of 4 KB \n" +
                "pos: 38912, i:10\n" +
                "touchPage 13 til 13 count 1\n" +
                "debug none: Advanced 4 KB, avg 3 KB between pretouch() and 2 KB while mapping of 4 KB \n", record.toString());
    }

    class DummyPretoucherState extends PretoucherState {
        private final BooleanSupplier touched;
        StringBuilder record;
        boolean first = true;
        private long last;

        DummyPretoucherState(@NotNull LongSupplier posSupplier, int headRoom, StringBuilder record, BooleanSupplier touched) {
            super(posSupplier, headRoom);
            this.record = record;
            this.touched = touched;
        }

        @Override
        protected void debug(String message) {
            record.append("debug ").append(message).append("\n");
        }

        @Override
        protected boolean touchPage(MappedBytes bytes, long offset) {
            if (first) {
                record.append("touchPage ").append(offset / 4096);
                first = false;
            }
            last = offset;
            return touched.getAsBoolean();
        }

        @Override
        protected void onTouched(int count) {
            record.append(" til ").append(last / 4096).append(" count ").append(count).append("\n");
            first = true;
        }
    }
}