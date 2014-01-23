/*
 * Copyright 2013 Peter Lawrey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.sandbox;

import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;

public class DateCacheTest {
    @Test
    public void testFormat() {
        DateCache dc = new DateCache("yyyyMMdd", 86400000);
        String str = dc.formatFor(16067);
        assertEquals("20131228", str);
        String str1 = dc.formatFor(1);
        assertEquals("19700102", str1);
    }

    @Test
    public void testFormatMillis() {
        String format = "yyyyMMddHHmmss";
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));

        DateCache dc = new DateCache(format, 1000);

        int now = (int) (System.currentTimeMillis() / 1000);
        for (int i = 0; i < 10000; i++) {
            int now2 = now + i;
            String str2 = sdf.format(new Date(now2 * 1000L));
            String str = dc.formatFor(now2);
            assertEquals("i: " + i, str2, str);
        }
    }
}
