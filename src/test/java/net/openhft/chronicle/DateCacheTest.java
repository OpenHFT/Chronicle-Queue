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

package net.openhft.chronicle;

import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;

public class DateCacheTest {
    private static final TimeZone GMT = TimeZone.getTimeZone("GMT");

    @Test
    public void testFormat() {
        VanillaDateCache dc = new VanillaDateCache("/tmp","yyyyMMdd", 86400000, GMT);
        String str = dc.valueFor(16067).text;
        assertEquals("20131228", str);
        String str1 = dc.valueFor(1).text;
        assertEquals("19700102", str1);
    }

    @Test
    public void testFormatMillis() {
        String format = "yyyyMMddHHmmss";
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));

        VanillaDateCache dc = new VanillaDateCache("/tmp",format, 1000, GMT);

        int now = (int) (System.currentTimeMillis() / 1000);
        for (int i = 0; i < 10000; i++) {
            int now2 = now + i;
            String str2 = sdf.format(new Date(now2 * 1000L));
            String str = dc.valueFor(now2).text;
            assertEquals("i: " + i, str2, str);
        }
    }
}
